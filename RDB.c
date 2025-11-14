#include "kvstore.h"
#define BUFFER_LENGTH 4096
#if ENABLE_ARRAY
extern kvs_array_t global_array;
#endif

#if ENABLE_RBTREE
extern kvs_rbtree_t global_rbtree;
#endif

#if ENABLE_HASH
extern kvs_hash_t global_hash;
#endif

int kvs_rdb_save(){
#if ENABLE_ARRAY    
    char *array_filename="data_files/array_rdb.rdb";
    int ret_array= kvs_array_save(array_filename,&global_array);
    if(ret_array!=0){
        printf("array_rdb save failed\n");
    }else{
        //printf("array_rdb save succeeded\n");
    }
#endif

#if ENABLE_RBTREE
    char *rbtree_filename="data_files/rbtree_rdb.rdb";
    int ret_rbtree= kvs_rbtree_save(rbtree_filename,&global_rbtree);
    if(ret_rbtree!=0){
        printf("rbtree_rdb save failed\n");
    }else{
        //printf("rbtree_rdb save succeeded\n");
    }
#endif
#if ENABLE_HASH
    char *hash_filename="data_files/hash_rdb.rdb";
    int ret_hash=kvs_hash_save(hash_filename,&global_hash);
    if(ret_hash!=0){
        printf("hahs_rdb saved failed\n");
    }else{
        //printf("hash_rdb saved succeeded\n");
    }
#endif
    return 0;
}

int kvs_rdb_load(){

#if ENABLE_ARRAY    
    int array_ret=kvs_array_load("data_files/array_rdb.rdb",&global_array);
    if(array_ret!=0){
        printf("array_rdb loaded failed\n");
    }else{
        printf("array_rdb loaded succeeded\n");
    }
#endif
#if ENABLE_RBTREE
    int rbtree_ret=kvs_rbtree_load("data_files/rbtree_rdb.rdb",&global_rbtree);
    if(rbtree_ret!=0){
        printf("rbtree_rdb loaded failed\n");
    }else{
        printf("rbtree_rdb loaded succeeded\n");
    }
#endif
#if ENABLE_HASH
    int hash_ret=kvs_hash_load("data_files/hash_rdb.rdb",&global_hash);
    if(hash_ret!=0){
        printf("hash_rddb loaded failed\n");
    }else{
        printf("hash_rdb loaded succeeded\n");
    }
#endif

    return 0;
}

//array save
int kvs_array_save(char*filename,kvs_array_t *inst){
    char tmpfile[256];
    pid_t pid = getpid();
    snprintf(tmpfile,sizeof(tmpfile),"%s.tmp.%d",filename,pid);
    
    FILE *fp=fopen(tmpfile,"wb");
    if(!fp){
        perror("open rdb file failed\n");
        return -1;
    }

    fwrite("array_rdb",1,10,fp);//写入头文件
    int version=1;
    fwrite(&version,sizeof(int),1,fp);//版本信息
    for(int i=0;i<inst->total;i++){//写入key value
		if (inst->table[i].key == NULL) {
			continue;
		}
        kvs_array_item_t *p=inst->table;
        int klen=strlen(p[i].key);
        int vlen=strlen(p[i].value);
        fwrite(&klen,sizeof(int),1,fp);
        fwrite(p[i].key,1,klen,fp);

        fwrite(&vlen,sizeof(int),1,fp);
        fwrite(p[i].value,1,vlen,fp);
    }
    unsigned char eof=0xFF;
    fwrite(&eof,1,1,fp);//写入文件结束标记

    fclose(fp);//关闭文件

    //remove(filename);
    rename(tmpfile,filename);//替换文件

    //printf("[RDB] Saved %d keys to %s\n", inst->total, filename);
    return 0;
}


//array load
int kvs_array_load(char *filename,kvs_array_t *inst){
    FILE *fp=fopen(filename,"rb");
    if(!fp){
        perror("open array_rdb file failed");
        return -1;
    }

    char head[10]={0};
    fread(head,1,10,fp);
    if(strcmp(head,"array_rdb")!=0){
        printf("[array_rdb] invalid header");
        fclose(fp);
        return -1;
    }

    int version=0;
    fread(&version,sizeof(int),1,fp);
    printf("[array_rdb] version=%d",version);
    while(1){
        int klen=0;
        if(fread(&klen,sizeof(int),1,fp)!=1) break;
        if(klen==0xFF) break;

        char *key=kvs_malloc(klen+1);
        fread(key,1,klen,fp);
        key[klen]='\0';

        int vlen=0;
        fread(&vlen,sizeof(int),1,fp);
        char *val= kvs_malloc(vlen+1);
        fread(val,1,vlen,fp);
        val[vlen]='\0';
        kvs_array_set(inst,key,val);
    }
    fclose(fp);
    printf("[array_rdb] loaded %d keys from %s\n",inst->total,filename);
    return 0;

}


//rbtree save
int kvs_rbtree_save(char* filename, rbtree *T) {
    char tmpfile[256];
    pid_t pid = getpid();
    snprintf(tmpfile,sizeof(tmpfile),"%s.tmp.%d",filename,pid);

    FILE *fp = fopen(tmpfile, "wb");
    if (!fp) {
        perror("open rdb file failed");
        return -1;
    }

    fwrite("rbtree_rdb", 1, 11, fp);
    int version = 1;
    fwrite(&version, sizeof(int), 1, fp);

    rbtree_node *current = T->root;
    rbtree_node **stack = kvs_malloc(TREE_LENGTH * sizeof(rbtree_node *));
    int top = 0;
    int rb_count = 0;

    // ---- 中序遍历 ----
    while (current != T->nil || top > 0) {

        // 一直向左压栈
        while (current != T->nil) {
            stack[top++] = current;
            current = current->left;
        }

        // 弹出一个节点
        current = stack[--top];

        // 访问节点（写入文件）
        int klen = strlen(current->key);
        int vlen = strlen(current->value);

        fwrite(&klen, sizeof(int), 1, fp);
        fwrite(current->key, 1, klen, fp);

        fwrite(&vlen, sizeof(int), 1, fp);
        fwrite(current->value, 1, vlen, fp);

        rb_count++;

        // 转向右子树
        current = current->right;
    }

    unsigned char eof = 0xFF;
    fwrite(&eof, 1, 1, fp);

    fclose(fp);
    kvs_free(stack);

    //remove(filename);
    rename(tmpfile, filename);

    //printf("[RDB] Saved %d keys to %s\n", rb_count, filename);
    return 0;
}


int kvs_rbtree_load(char*filename,rbtree *T){
    FILE *fp=fopen(filename,"rb");
    if(!fp){
        perror("open rbtree_rdb file failed");
        return -1;
    }

    char head[11]={0};
    fread(head,1,11,fp);
    if(strcmp(head,"rbtree_rdb")!=0){
        printf("[rbtree_rdb] invalid header");
        fclose(fp);
        return -1;
    }

    int version=0;
    fread(&version,sizeof(int),1,fp);
    printf("[rbtree_rdb] version=%d",version);
    int total=0;
    while(1){
        int klen=0;
        if(fread(&klen,sizeof(int),1,fp)!=1) break;
        if(klen==0xFF) break;

        char *key=kvs_malloc(klen+1);
        fread(key,1,klen,fp);
        key[klen]='\0';

        int vlen=0;
        fread(&vlen,sizeof(int),1,fp);
        char *val= kvs_malloc(vlen+1);
        fread(val,1,vlen,fp);
        val[vlen]='\0';
        kvs_rbtree_set(T,key,val); 
        total++;   
    }
    fclose(fp);
    printf("[rbtree_rdb] loaded %d keys from %s\n",total,filename);
    return 0;
}

int kvs_hash_save(char* filename,kvs_hash_t *hash){

    char tmpfile[256];
    pid_t pid = getpid();
    snprintf(tmpfile,sizeof(tmpfile),"%s.tmp.%d",filename,pid);
    
    FILE *fp=fopen(tmpfile,"wb");
    if(!fp){
        perror("open rdb file failed\n");
        return -1;
    }

    fwrite("hash_rdb",1,8,fp);//写入头文件
    int version=1;
    fwrite(&version,sizeof(int),1,fp);//版本信息
   
    int hash_count=0;
	int i = 0;
	for (i = 0;i < hash->max_slots;i ++) {
		hashnode_t *node = hash->nodes[i];

		while (node != NULL) { // error
            hash_count++;

            int klen=strlen(node->key);
            int vlen=strlen(node->value);

            fwrite(&klen,sizeof(int),1,fp);
            fwrite(node->key,1,klen,fp);

            fwrite(&vlen,sizeof(int),1,fp);
            fwrite(node->value,1,vlen,fp);
			
			node = node->next;
		}
	}

    unsigned char eof=0xFF;
    fwrite(&eof,1,1,fp);//写入文件结束标记

    fclose(fp);//关闭文件

    remove(filename);
    rename(tmpfile,filename);//替换文件

   // printf("[RDB] Saved %d keys to %s\n", hash_count, filename);
    return 0;


}


int kvs_hash_load(char* filename,kvs_hash_t *hash){
    FILE* fp=fopen(filename,"rb");
    if(!fp){
        perror("open hash_rdb file failed\n");
        return -1;
    }
    char head[8]={0};
    fread(head,1,8,fp);
    if(strcmp(head,"hash_rdb")!=0){
        printf("[hash_rdb] invalid header");
        fclose(fp);
        return -1;
    }

    int version=0;
    fread(&version,sizeof(int),1,fp);
    printf("[hash_rdb] version=%d",version);
    int total=0;
    while(1){
        int klen=0;
        if(fread(&klen,sizeof(int),1,fp)!=1) break;
        if(klen==0xFF) break;

        char* key=kvs_malloc(klen+1);
        fread(key,1,klen,fp);
        key[klen]='\0';

        int vlen=0;
        fread(&vlen,sizeof(int),1,fp);
        char *val=kvs_malloc(vlen+1);
        fread(val,1,vlen,fp);
        val[vlen]='\0';
        kvs_hash_set(hash,key,val);
        total++;
    }
    fclose(fp);
    printf("[hash_rdb] loaded %d keys from %s\n",total,filename);
    return 0;
}

extern int proactor_broadcast(char *msg, size_t len);

int kvs_rdb_broadcast_all() {
    printf("start rdb broadcast all\n");
#if ENABLE_ARRAY
    for (int i = 0; i < global_array.total; i++) {
        kvs_array_item_t *p = &global_array.table[i];
        if (p->key == NULL || p->value == NULL) continue;
        char *msg=kvs_malloc(BUFFER_LENGTH);
        char *tokens[KVS_MAX_TOKENS] = {0};
        tokens[0]="SYNC";
        tokens[1]="SET";
        tokens[2]=p->key;
        tokens[3]=p->value;
        int n=kvs_join_tokens(tokens,4,msg);
        //printf("send:%s\n",msg);
        //int n = snprintf(msg, BUFFER_LENGTH, "SET %s %s\r\n", p->key, p->value);
        //proactor_broadcast(msg, n);
#if (NETWORK_SELECT == NETWORK_REACTOR)
			//reactor_broadcast(msg,n);
            extern int client_fds[MAX_CLIENTS];
            extern int client_count;
            for (int i = 0; i < client_count; i++) {
                int fd = client_fds[i]; 
                int ret=send(fd,msg,n,0);
                printf("ret: %d, fd:  %d msg %s",ret,fd,msg);
                char result[1024]={0};
                int res=recv(fd,result,1024,0);
                if(strcmp(result,"SYNCC completed\r\n")==0){
                    printf("SYNCC completed\r\n");
                }
            }           
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
			proactor_broadcast(msg, n);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
			ntyco_broadcast(msg,n);
#endif
    }
    printf("[rdb_broadcast] sent %d array kvs\n", global_array.total);
#endif

#if ENABLE_RBTREE
    rbtree_node *current = global_rbtree.root;
    rbtree_node **stack = (rbtree_node **)malloc(1024 * sizeof(rbtree_node *));
    int top = 0, count = 0;

    while (current != global_rbtree.nil || top > 0) {
        while (current != global_rbtree.nil) {
            stack[top++] = current;
            current = current->left;
        }
        current = stack[--top];
        if (current->key && current->value) {
            char *msg2=kvs_malloc(BUFFER_LENGTH);
            char *tokens[KVS_MAX_TOKENS] = {0};
            tokens[0]="SYNC";
            tokens[1]="RSET";
            tokens[2]=current->key;
            tokens[3]=current->value;
            int n=kvs_join_tokens(tokens,4,msg2);
#if (NETWORK_SELECT == NETWORK_REACTOR)
			//reactor_broadcast(msg,n);
            int relen=strlen(reactor_response);
            memcpy(reactor_response+relen,msg2,strlen(msg2));
            //printf("re_msg:%s\n",reactor_response);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
			proactor_broadcast(msg2, n);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
			ntyco_broadcast(msg2,n);
#endif
            count++;
        }
        current = current->right;
    }

    free(stack);
    printf("[rdb_broadcast] sent %d rbtree kvs\n", count);
#endif

#if ENABLE_HASH
    int total = 0;
    for (int i = 0; i < global_hash.max_slots; i++) {
        hashnode_t *node = global_hash.nodes[i];
        while (node != NULL) {
            
            //int n = snprintf(msg3, BUFFER_LENGTH, "HSET %s %s\r\n", node->key, node->value);
            char *msg3=kvs_malloc(BUFFER_LENGTH);
            char *tokens[KVS_MAX_TOKENS] = {0};
            tokens[0]="SYNC";
            tokens[1]="HSET";
            tokens[2]=node->key;
            tokens[3]=node->value;
            int n=kvs_join_tokens(tokens,4,msg3);
#if (NETWORK_SELECT == NETWORK_REACTOR)
			//reactor_broadcast(msg,n);
            int relen=strlen(reactor_response);
            memcpy(reactor_response+relen,msg3,strlen(msg3));
            //printf("re_msg:%s\n",reactor_response);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
			proactor_broadcast(msg3, n);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
			ntyco_broadcast(msg3,n);
#endif
            node = node->next;
            total++;
        }
    }
    printf("[rdb_broadcast] sent %d hash kvs\n", total);
#endif

    return 0;
}

extern void print_visible(char *msg);
#if 0
int kvs_rdb_reactor_broadcast_all(char* response) {
    printf("start rdb broadcast all\n");
    memset(response, 0, sizeof(response));
#if ENABLE_ARRAY
    for (int i = 0; i < global_array.total; i++) {
        kvs_array_item_t *p = &global_array.table[i];
        if (p->key == NULL || p->value == NULL) continue;
        char *msg=kvs_malloc(BUFFER_LENGTH);
        char *tokens[KVS_MAX_TOKENS] = {0};
        tokens[0]="SYNC";
        tokens[1]="SET";
        tokens[2]=p->key;
        tokens[3]=p->value;
        int n=kvs_join_tokens(tokens,4,msg);
        //int n = snprintf(msg, BUFFER_LENGTH, "SET %s %s\r\n", p->key, p->value);
        //proactor_broadcast(msg, n);
#if (NETWORK_SELECT == NETWORK_REACTOR)
			//reactor_broadcast(msg,n);
            //printf("re_msg:%s \n",reactor_response);
            int relen=strlen(response);
            printf("\nresp:");
            print_visible(response);
            memcpy(response+relen,msg,strlen(msg));
            //printf("re_msg:%s\n",response);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
			proactor_broadcast(msg, n);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
			ntyco_broadcast(msg,n);
#endif
    }
    printf("[rdb_broadcast] sent %d array kvs\n", global_array.total);
#endif

#if ENABLE_RBTREE
    rbtree_node *current = global_rbtree.root;
    rbtree_node **stack = (rbtree_node **)malloc(1024 * sizeof(rbtree_node *));
    int top = 0, count = 0;

    while (current != global_rbtree.nil || top > 0) {
        while (current != global_rbtree.nil) {
            stack[top++] = current;
            current = current->left;
        }
        current = stack[--top];
        if (current->key && current->value) {
            char *msg2=kvs_malloc(BUFFER_LENGTH);
            char *tokens[KVS_MAX_TOKENS] = {0};
            tokens[0]="SYNC";
            tokens[1]="RSET";
            tokens[2]=current->key;
            tokens[3]=current->value;
            int n=kvs_join_tokens(tokens,4,msg2);
#if (NETWORK_SELECT == NETWORK_REACTOR)
			//reactor_broadcast(msg,n);
            int relen=strlen(response);
            memcpy(response+relen,msg2,strlen(msg2));
            //printf("re_msg:%s\n",response);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
			proactor_broadcast(msg2, n);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
			ntyco_broadcast(msg2,n);
#endif
            count++;
        }
        current = current->right;
    }

    free(stack);
    printf("[rdb_broadcast] sent %d rbtree kvs\n", count);
#endif

#if ENABLE_HASH
    int total = 0;
    for (int i = 0; i < global_hash.max_slots; i++) {
        hashnode_t *node = global_hash.nodes[i];
        while (node != NULL) {
            
            //int n = snprintf(msg3, BUFFER_LENGTH, "HSET %s %s\r\n", node->key, node->value);
            char *msg3=kvs_malloc(BUFFER_LENGTH);
            char *tokens[KVS_MAX_TOKENS] = {0};
            tokens[0]="SYNC";
            tokens[1]="HSET";
            tokens[2]=node->key;
            tokens[3]=node->value;
            int n=kvs_join_tokens(tokens,4,msg3);
#if (NETWORK_SELECT == NETWORK_REACTOR)
			//reactor_broadcast(msg,n);
            int relen=strlen(response);
            memcpy(response+relen,msg3,strlen(msg3));
            //printf("re_msg:%s\n",response);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
			proactor_broadcast(msg3, n);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
			ntyco_broadcast(msg3,n);
#endif
            node = node->next;
            total++;
        }
    }
    printf("[rdb_broadcast] sent %d hash kvs\n", total);
#endif

    return strlen(response);
}

#endif