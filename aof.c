#include "kvstore.h"
    #define AOF_DEBUG 0
    #if AOF_DEBUG
    #define AOF_PRINT(fmt, ...)  printf(fmt, ##__VA_ARGS__)
    #else
    #define AOF_PRINT(fmt, ...)  ((void)0)
    #endif
#define AOF_BUFFER_SIZE 10*10000
#if ENABLE_ARRAY
extern kvs_array_t global_array;
#endif

#if ENABLE_RBTREE
extern kvs_rbtree_t global_rbtree;
#endif

#if ENABLE_HASH
extern kvs_hash_t global_hash;
#endif
FILE *aof_fp=NULL;
char *aof_file="data_files/kvs_aof.aof";
int kvs_aof_open(char *filename){
    aof_fp=fopen(filename,"ab+");
    if(!aof_fp){
        perror("open aof failed\n");
        return -1;
    }
    return 0;
}
void kvs_aof_close(){
    if(aof_fp) fclose(aof_fp);
    aof_fp=NULL;
}
/* 初始化：打开AOF文件，仅执行一次 */
int kvs_aof_init(void) {
    aof_fp = fopen(aof_file, "ab+");
    if (!aof_fp) {
        perror("open aof failed");
        return -1;
    }
    setvbuf(aof_fp, NULL, _IOFBF, 8192); // 设置缓冲区（可选，提升性能）
    return 0;
}

/* 手动刷新缓冲区 */
void kvs_aof_flush(void) {
    if (aof_fp) fflush(aof_fp);
}
int kvs_aof_write(char *cmd,char *key,char *value){
    
    if (!aof_fp) {
        fprintf(stderr, "AOF file not opened!\n");
        return -1;
    }
    if(value!=NULL)
        fprintf(aof_fp, "%s %s %s\n", cmd, key, value);
    else
        fprintf(aof_fp, "%s %s\n", cmd, key);
    // 可以按需决定是否立即刷新：
    // fflush(aof_fp);
    AOF_PRINT("AOF write: %s %s %s\n", cmd, key, value);
    return 0;
}

int aof_split_token(char *msg, char *tokens[]) {

	if (msg == NULL || tokens == NULL) return -1;
    // 去除末尾的换行符或回车符
    size_t len = strlen(msg);
    while (len > 0 && (msg[len - 1] == '\n' || msg[len - 1] == '\r')) {
        msg[len - 1] = '\0';
        len--;
    }
	int idx = 0;
	char *token = strtok(msg, " ");
	
	while (token != NULL) {
		//printf("idx: %d, %s\n", idx, token);
		
		tokens[idx ++] = token;
		token = strtok(NULL, " ");
	}

	return idx;
}

// int kvs_aof_load(){
//     FILE *fp=fopen(aof_file,"r");
//     if(!fp) return 0;
//     char *tokens[KVS_MAX_TOKENS]={0};
//     char msg[AOF_BUFFER_SIZE];
//     int aof_count=0;
//     printf("start load aof\n");
//     while(fgets(msg,AOF_BUFFER_SIZE,fp)!=NULL){
//         if(aof_split_token(msg,tokens)==0){
//             printf("split failed\n"); 
//             return -1;
//         }
//         aof_count++;
//         //tokens[2][strcspn(tokens[2], "\r\n")] = '\0';
//         //printf("%s\n",tokens[0]);
// #if ENABLE_ARRAY
//         if(strcmp(tokens[0],"SET")==0){
//             if(kvs_array_set(&global_array,tokens[1],tokens[2])==0)
//                ; //printf("aof loaded:SET %s %s\n",tokens[1],tokens[2]);
//             //else    
//                 //printf("aof loaded failed");
//         }
//         if(strcmp(tokens[0],"MOD")==0){
//             if(kvs_array_mod(&global_array,tokens[1],tokens[2])==0)
//               ; // printf("aof loaded:MOD %s %s\n",tokens[1],tokens[2]);
//            // else    
//                 //printf("aof loaded failed");
//         }
//         if(strcmp(tokens[0],"DEL")==0){
//             if(kvs_array_del(&global_array,tokens[1])==0)
//                ;// printf("aof loaded:DEL %s\n",tokens[1]);
//             //else    
//                 //printf("aof loaded failed");
//         }
// #endif
// #if ENABLE_RBTREE
//         if(strcmp(tokens[0],"RSET")==0){
//             if(kvs_rbtree_set(&global_rbtree,tokens[1],tokens[2])==0)
//                 ;//printf("aof loaded:RSET %s %s\n",tokens[1],tokens[2]);
//             //else    
//                // printf("aof loaded failed");
//         }
//         if(strcmp(tokens[0],"RMOD")==0){
//             if(kvs_rbtree_mod(&global_rbtree,tokens[1],tokens[2])==0)
//                 ;//printf("aof loaded:RMOD %s %s\n",tokens[1],tokens[2]);
//             //else    
//                 //printf("aof loaded failed");
//         }
//         if(strcmp(tokens[0],"RDEL")==0){
//             if(kvs_rbtree_del(&global_rbtree,tokens[1])==0)
//                ;// printf("aof loaded:RDEL %s\n",tokens[1]);
//             //else    
//                 //printf("aof loaded failed");
//         }  
// #endif
// #if ENABLE_HASH
//         if(strcmp(tokens[0],"HSET")==0){
//             if(kvs_hash_set(&global_hash,tokens[1],tokens[2])==0)
//                 ;//printf("aof loaded:HSET %s %s\n",tokens[1],tokens[2]);
//             //else    
//                // printf("aof loaded failed");
//         }
//         if(strcmp(tokens[0],"HMOD")==0){
//             if(kvs_hash_mod(&global_hash,tokens[1],tokens[2])==0)
//                ;// printf("aof loaded:HMOD %s %s\n",tokens[1],tokens[2]);
//            // else    
//                 //printf("aof loaded failed");
//         }
//         if(strcmp(tokens[0],"HDEL")==0){
//             if(kvs_hash_del(&global_hash,tokens[1])==0)
//               ; // printf("aof loaded:HDEL %s\n",tokens[1]);
//             //else    
//                // printf("aof loaded failed");
//         }
// #endif
//     }
//     printf("laod aof file %d succeeded\n",aof_count);
//     fclose(fp);
//     return 0;
// }
    // === 控制调试输出开关 ===
    // 改成 1 开启打印，改成 0 关闭打印

    // =======================
int kvs_aof_load() {
    AOF_PRINT("Start loading AOF file: %s\n", aof_file);
    FILE *fp = fopen(aof_file, "r");
    
    if (!fp){
        printf("open file failed\n");
        return 0;
    } 



    char *tokens[KVS_MAX_TOKENS] = {0};
    char msg[AOF_BUFFER_SIZE];
    int aof_count = 0;

    

    while (fgets(msg, AOF_BUFFER_SIZE, fp) != NULL) {
        if (aof_split_token(msg, tokens) == 0) {
            AOF_PRINT("[Error] Split failed for line: %s\n", msg);
            fclose(fp);
            return -1;
        }
        aof_count++;

#if ENABLE_ARRAY
        if (strcmp(tokens[0], "SET") == 0) {
            if (kvs_array_set(&global_array, tokens[1], tokens[2]) == 0)
                AOF_PRINT("AOF loaded: SET %s %s\n", tokens[1], tokens[2]);
        } else if (strcmp(tokens[0], "MOD") == 0) {
            if (kvs_array_mod(&global_array, tokens[1], tokens[2]) == 0)
                AOF_PRINT("AOF loaded: MOD %s %s\n", tokens[1], tokens[2]);
        } else if (strcmp(tokens[0], "DEL") == 0) {
            if (kvs_array_del(&global_array, tokens[1]) == 0)
                AOF_PRINT("AOF loaded: DEL %s\n", tokens[1]);
        }
#endif

#if ENABLE_RBTREE
        if (strcmp(tokens[0], "RSET") == 0) {
            if (kvs_rbtree_set(&global_rbtree, tokens[1], tokens[2]) == 0)
                AOF_PRINT("AOF loaded: RSET %s %s\n", tokens[1], tokens[2]);
        } else if (strcmp(tokens[0], "RMOD") == 0) {
            if (kvs_rbtree_mod(&global_rbtree, tokens[1], tokens[2]) == 0)
                AOF_PRINT("AOF loaded: RMOD %s %s\n", tokens[1], tokens[2]);
        } else if (strcmp(tokens[0], "RDEL") == 0) {
            if (kvs_rbtree_del(&global_rbtree, tokens[1]) == 0)
                AOF_PRINT("AOF loaded: RDEL %s\n", tokens[1]);
        }
#endif

#if ENABLE_HASH
        if (strcmp(tokens[0], "HSET") == 0) {
            if (kvs_hash_set(&global_hash, tokens[1], tokens[2]) == 0)
                AOF_PRINT("AOF loaded: HSET %s %s\n", tokens[1], tokens[2]);
        } else if (strcmp(tokens[0], "HMOD") == 0) {
            if (kvs_hash_mod(&global_hash, tokens[1], tokens[2]) == 0)
                AOF_PRINT("AOF loaded: HMOD %s %s\n", tokens[1], tokens[2]);
        } else if (strcmp(tokens[0], "HDEL") == 0) {
            if (kvs_hash_del(&global_hash, tokens[1]) == 0)
                AOF_PRINT("AOF loaded: HDEL %s\n", tokens[1]);
        }
#endif
#if ENABLE_DATA
        if(strcmp(tokens[0],"LSET")==0){
            if(kvs_large_set(tokens[1],tokens[2])==0)
                AOF_PRINT("AOF laoded: LSET %s %s\n", tokens[1], tokens[2]);
        }else if(strcmp(tokens[0],"LMOD")==0){
            if(kvs_large_mod(tokens[1],tokens[2])==0)
                AOF_PRINT("AOF loaded: LMOD %s %s\n", tokens[1], tokens[2]);
        }else if (strcmp(tokens[0], "LDEL") == 0) {
            if (kvs_large_del(tokens[1]) == 0)
                AOF_PRINT("AOF loaded: LDEL %s\n", tokens[1]);
        }
#endif
    }

    printf("Loaded AOF file successfully, total %d entries\n", aof_count);
    fclose(fp);
    return 0;
}
