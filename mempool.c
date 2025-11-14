#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>
#include<stddef.h>
#include"kvstore.h"
#if NEW_MALLOC
#define PAGE_SIZE 4096
#define RUN_SIZE (4*1024)//每次分配64k
#define SIZE_CLASS_COUNT 8//尺寸分类数量
static const size_t size_classes[SIZE_CLASS_COUNT] = {
    8, 16, 32, 64, 128, 256, 512, 1024
};

#define SIZE_MAX_SMALL 1024 //小内存上限

typedef struct block_header{
    uint32_t magic;//魔数，校验block有效性
    uint32_t size;//尺寸
}block_header_t;//区别小分配和大分配，小分配则分配size_classes

/* 魔数定义 */
#define HEADER_MAGIC 0xDEADBEEF   /* 小 block 魔数 */
#define LARGE_MAGIC  0xABCD1234   /* 大 block 魔数 */

//
typedef struct free_node{
    struct free_node *next;
}free_node_t;//空闲链表

typedef struct size_bin{
    size_t class_size;//该尺寸可用字节数
    free_node_t *free_list;//空闲链表头
}size_bin_t;//每一个大小的block维护一个空闲链表


static size_bin_t bins[SIZE_CLASS_COUNT];
static int allocator_inited=0;

//没懂，待查
static inline size_t align_up(size_t v,size_t a){
    return (v+a-1)&~(a-1);
}
//根据请求的大小分配一个最近似的bin
static int size_to_index(size_t size){
    for(int i=0;i<SIZE_CLASS_COUNT;i++){
        if(size<=size_classes[i])
            return i;
    }
    return -1;
}
//初始化所有size class
static void kvs_malloc_init(void){
    if(allocator_inited) return;
    for(int i=0;i<SIZE_CLASS_COUNT;i++){
        bins[i].class_size=size_classes[i];
        bins[i].free_list=NULL;
    }
    allocator_inited=1;
    //printf("malloc_init succeeded\n");
}

static int populate_run(int idx){
    size_t block_payload= bins[idx].class_size;//获取当前size
    size_t block_total= sizeof(block_header_t)+block_payload;//block总大小
    block_total=align_up(block_total,sizeof(void*));//向上对齐

    size_t run_bytes=RUN_SIZE;
    if(run_bytes<block_total) run_bytes=block_total;

    void *run=malloc(run_bytes);
    if(!run) return -1;
    
    size_t offset=0;
    free_node_t *head=bins[idx].free_list;//获取空闲链表头

    while(offset+block_total<=run_bytes){//将申请到的page按size大小分块
        char *block_base=(char*)run+offset;
        block_header_t *hdr=(block_header_t*)block_base;
        hdr->magic=HEADER_MAGIC;
        hdr->size=(uint32_t)bins[idx].class_size;

        void* user_ptr=block_base+sizeof(block_header_t);
        free_node_t *node=(free_node_t*)user_ptr;
        
        node->next=head;
        head=node;
        offset+=block_total;
    }

    bins[idx].free_list=head;
    return 0;
}

void *kvs_malloc(size_t size){
    kvs_malloc_init();
    if(size==0) size=1;
    if(size<=SIZE_MAX_SMALL){
        int idx=size_to_index(size);
        if(idx<0)idx=SIZE_CLASS_COUNT-1;
        if(!bins[idx].free_list){
            if(populate_run(idx)!=0)return NULL;
        }
        free_node_t *node=bins[idx].free_list;
        bins[idx].free_list=node->next;
        return (void*)node;//通过populate_run申请小块内存并返回
    }

    size_t total=sizeof(block_header_t)+size;
    void *p=malloc(total);
    if(!p) return NULL;
    block_header_t *hdr=(block_header_t *)p;
    hdr->magic=LARGE_MAGIC;
    hdr->size=(uint32_t)size;
    return (char*)p+sizeof(block_header_t);//返回大内存块并记录
}


void kvs_free(void*ptr){
    if(!ptr)return;
    block_header_t *hdr=(block_header_t*)((char*)ptr-sizeof(block_header_t));//用户空间位置减头指针大小获得头指针位置
    if(hdr->magic==HEADER_MAGIC){
        int idx=size_to_index(hdr->size);
        if(idx<0){
            free(hdr);
            return;
        }
        free_node_t* node=(free_node_t*)ptr;
        node->next=bins[idx].free_list;
        bins[idx].free_list=node;//将需要释放的block放到可用链表的头部，表明该空间可用

    }else if(hdr->magic==LARGE_MAGIC){
        free((void*)hdr);//大空间内存直接释放
    }
    else{
        fprintf(stderr,"free: invalid magic（疑似双重释放或内存破坏！）\n");
    }
}

//分配空间并清0
void *je_calloc(size_t nmemb,size_t size){
    size_t total=nmemb*size;
    void* p=kvs_malloc(total);
    if(p)memset(p,0,total);
    return p;
}

//重新分配大小
void *je_realloc(void* ptr,size_t new_size){
    if(!ptr)return kvs_malloc(new_size);
    if(new_size==0){
        kvs_free(ptr);
        return NULL;
    }
    block_header_t *hdr=(block_header_t*)((char*)ptr-sizeof(block_header_t));
    size_t old_size=hdr->size;

    if(hdr->magic==HEADER_MAGIC){
        if(new_size<=old_size)return ptr;
        void* newp=kvs_malloc(new_size);
        if(!newp) return NULL;
        memcpy(newp,ptr,old_size);
        kvs_free(ptr);
        return newp;
    }
    else if(hdr->magic==LARGE_MAGIC){
        void* newp=kvs_malloc(new_size);
        if(!newp) return NULL;
        memcpy(newp,ptr,old_size<new_size?old_size:new_size);
        kvs_free(ptr);
        return newp;
    }
    fprintf(stderr,"je_realloc:invalid header\n");
    return NULL;
}

/* 输出空闲链表统计信息 */
void jemalloc_stats(void) {
    printf("jemalloc stats:\n");
    for (int i = 0; i < SIZE_CLASS_COUNT; i++) {
        int free_cnt = 0;
        free_node_t *n = bins[i].free_list;
        while (n && free_cnt < 1000000) {
            free_cnt++;
            n = n->next;
        }
        printf(" class %4zu: free=%d\n",
               bins[i].class_size, free_cnt);
    }
}

#define SIM_JEMALLOC_TEST 0
#if SIM_JEMALLOC_TEST
int main(void) {
    
    printf("sim_jemalloc test\n");
    void *a = kvs_malloc(10);
    void *b = kvs_malloc(15);
    void *c = kvs_malloc(15);

    printf("a:%p\nb:%p\n",a,b);
    jemalloc_stats();
    strcpy((char *)a, "hello");
    strcpy((char *)b, "world");
    printf("a='%s' b='%s' c ptr=%p\n",
           (char *)a, (char *)b, c);
    printf("a:%p\nb:%p\n",a,b);
    kvs_free(a);
    kvs_free(b);
    kvs_free(c);

    jemalloc_stats();
    return 0;
}
#endif

#endif