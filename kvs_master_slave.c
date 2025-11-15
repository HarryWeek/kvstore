#include "kvstore.h"

#if ENABLE_MS
extern kvs_array_t global_array;
extern kvs_rbtree_t global_rbtree;
extern kvs_hash_t global_hash;
extern int client_fds[MAX_CLIENTS];
extern int client_count;
int kvs_ms_filter_protocol(char **tokens, int count) {
    if (tokens == NULL || count == 0) return -1;
    
    //printf("cmd: %s\n",tokens[1]);
    int cmd = -1;
    const char *cmds[] = {
        "SET","DEL","MOD",
        "RSET","RDEL","RMOD",
        "HSET","HDEL","HMOD",
        "LSET","LDEL","LMOD"
    };

    for (int i = 0; i < sizeof(cmds)/sizeof(cmds[1]); i++) {
        if (strcmp(tokens[1], cmds[i]) == 0) {
            cmd = i;
            break;
        }
    }

    if (cmd == -1) {
        printf("[SYNC] Unknown cmd: %s\n", tokens[1]);
        return -1;
    }

    char *key = (count > 1) ? tokens[2] : NULL;
    char *value = (count > 2) ? tokens[3] : NULL;

    // 执行操作，但不返回任何字符串，也不广播
    switch (cmd) {
#if ENABLE_ARRAY
        case 0: kvs_array_set(&global_array, key, value); break; // SET
        case 1: kvs_array_del(&global_array, key); break;        // DEL
        case 2: kvs_array_mod(&global_array, key, value); break; // MOD
#endif
#if ENABLE_RBTREE
        case 3: kvs_rbtree_set(&global_rbtree, key, value); break;
        case 4: kvs_rbtree_del(&global_rbtree, key); break;
        case 5: kvs_rbtree_mod(&global_rbtree, key, value); break;
#endif
#if ENABLE_HASH
        case 6: kvs_hash_set(&global_hash, key, value); break;
        case 7: kvs_hash_del(&global_hash, key); break;
        case 8: kvs_hash_mod(&global_hash, key, value); break;
#endif
#if ENABLE_DATA
        case 9:  kvs_large_set(key, value); break;
        case 10: kvs_large_del(key); break;
        case 11: kvs_large_mod(key, value); break;
#endif
        //case 12: kvs_rdb_broadcast_all();break;
        default: 
            printf("[SYNC] Unsupported op: %s\n", tokens[0]);
            break;
    }

#if ENABLE_RDB
    kvs_rdb_save();
#endif
    return 0;
}


/*
 * 主从同步协议处理（不返回response）
 * msg: 同步指令字符串
 * len: 长度
 */
int kvs_ms_protocol(char *msg, int len,char*response) {
    if (msg == NULL || len <= 0) return -1;
    printf("get msg from other end:%s\n",msg);
    
    char *tokens[KVS_MAX_TOKENS] = {0};
    int count = kvs_split_token(msg, tokens);
    if (count < 1) return -1;

    return kvs_ms_filter_protocol(tokens, count);
}
#endif
extern struct io_uring ring; 
int kvs_sync_msg(char* msg,int len){
    if(!msg||len<=0) return -1;
    if(client_count<=0) return -1;
if(NETWORK_SELECT==NETWORK_REACTOR){
    reactor_broadcast(msg,len);
}else if(NETWORK_SELECT==NETWORK_PROACTOR){
        for (int i = 0; i < client_count; i++) {
        int fd = client_fds[i]; 
        int ret=proactor_broadcast(msg,len);
        printf("ret: %d, fd:  %d msg %s",ret,fd,msg);
        char result[1024]={0};
        int res=set_event_recv(&ring,fd,result,1024,0);
        if(strcmp(result,"SYNCC completed\r\n")==0){
            printf("SYNCC completed\r\n");
        }else{
            return -1;
        }
    } 
}else if(NETWORK_SELECT==NETWORK_NTYCO){
    ntyco_broadcast(msg,len);
}
     
    return 0;
}