



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
#if OLD_MALLOC
void *kvs_malloc(size_t size) {
	return malloc(size);
}

void kvs_free(void *ptr) {
	return free(ptr);
}
#endif

const char *command[] = {
	"SET", "GET", "DEL", "MOD", "EXIST",
	"RSET", "RGET", "RDEL", "RMOD", "REXIST",
	"HSET", "HGET", "HDEL", "HMOD", "HEXIST",
	"LSET", "LGET", "LDEL", "LMOD", "LEXIST","SYNCALL",	
};

enum {
	KVS_CMD_START = 0,
	// array
	KVS_CMD_SET = KVS_CMD_START,
	KVS_CMD_GET,
	KVS_CMD_DEL,
	KVS_CMD_MOD,
	KVS_CMD_EXIST,
	// rbtree
	KVS_CMD_RSET,
	KVS_CMD_RGET,
	KVS_CMD_RDEL,
	KVS_CMD_RMOD,
	KVS_CMD_REXIST,
	// hash
	KVS_CMD_HSET,
	KVS_CMD_HGET,
	KVS_CMD_HDEL,
	KVS_CMD_HMOD,
	KVS_CMD_HEXIST,
	//larg data
	KVS_CMD_LSET,
	KVS_CMD_LGET,
	KVS_CMD_LDEL,
	KVS_CMD_LMOD,
	KVS_CMD_LEXIST,
	//主从同步
	KVS_CMD_SYNC,

	KVS_CMD_COUNT,
};



const char *response[] = {

};

/*
int kvs_split_token(char *msg, char *tokens[]) {

	if (msg == NULL || tokens == NULL) return -1;

	int idx = 0;
	char *token = strtok(msg, " ");
	
	while (token != NULL) {
		//printf("idx: %d, %s\n", idx, token);
		
		tokens[idx ++] = token;
		token = strtok(NULL, " ");
	}

	return idx;
}*/
#if 0
int max_tokens=4;
int kvs_split_token(char *msg, char *tokens[]) {
	 if (!msg || !tokens) return -1;

    char *p = msg;
    int argc = 0;

    // 检查开头
    if (*p != '*') {
        //fprintf(stderr, "Invalid RESP: must start with '*' p:[%c]\n",*p);
        return -1;
    }
	//while(*p!='*'||*p!='\0') p++;
    //  读取参数个数
    p++;
    int arg_count = atoi(p);

    // 找到第一个 "\r\n"
    char *rn = strstr(p, "\r\n");
    if (!rn) return -1;
    p = rn + 2;  // 跳过 "\r\n"

    // 记录起始位置（用于计算总长度）
    const char *start = msg;

    // 3️⃣ 解析每个参数
    for (int i = 0; i < arg_count && argc < max_tokens; i++) {
        if (*p != '$') {
            //fprintf(stderr, "Invalid RESP: expected '$' before arg\n");
            return -1;
        }
        p++; // skip '$'

        int len = atoi(p);
        rn = strstr(p, "\r\n");
        if (!rn) return -1;

        p = rn + 2; // move to actual string

        tokens[argc] = (char*)malloc(len + 1);
        memcpy(tokens[argc], p, len);
        tokens[argc][len] = '\0';
        argc++;

        p += len + 2; // 跳过 data + \r\n
    }

    // 计算总长度
    int total_len = (int)(p - start);
    return total_len;
}
int kvs_join_tokens(char *tokens[], int count, char *msg) {
    if (!tokens || !msg || count <= 0) return -1;

    char *p = msg;

    // 1️⃣ 写入参数个数
    p += sprintf(p, "*%d\r\n", count);

    // 2️⃣ 写入每个参数的 $len + 内容
    for (int i = 0; i < count; i++) {
        if (!tokens[i]) continue;
        int len = (int)strlen(tokens[i]);
        p += sprintf(p, "$%d\r\n", len);
        memcpy(p, tokens[i], len);
        p += len;
        memcpy(p, "\r\n", 2);
        p += 2;
    }

    *p = '\0'; // 结束符
    return p-msg;
}
#endif
// SET Key Value
// tokens[0] : SET
// tokens[1] : Key
// tokens[2] : Value

int kvs_filter_protocol(char **tokens, int count, char *response) {

	if (tokens[0] == NULL || count == 0 || response == NULL) return -1;
	//printf("cmd:%s\n",tokens[0]);



	int cmd = KVS_CMD_START;
	for (cmd = KVS_CMD_START;cmd < KVS_CMD_COUNT;cmd ++) {
		if (strcmp(tokens[0], command[cmd]) == 0) {
			break;
		} 
	}

	int length = 0;
	int ret = 0;
	char *key = tokens[1];
	char *value = tokens[2];
	char *send_token[KVS_MAX_TOKENS] = {0};
	send_token[0]="SYNC";
	send_token[1]=tokens[0];
	send_token[2]=tokens[1];
	send_token[3]=tokens[2];
	switch(cmd) {
#if ENABLE_ARRAY
	case KVS_CMD_SET:

			ret = kvs_array_set(&global_array ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
		} else if (ret == 0) {
if (ENABLE_AOF){
            kvs_aof_write("SET",key,value);
}
#if ENABLE_MS
			char send_buffer[BUFFER_LENGTH];
			int n=kvs_join_tokens(send_token,4,send_buffer);
			//printf("prepare to send:%s %s %s\n",send_token[1],send_token[2],send_token[3]);
			kvs_sync_msg(send_buffer,n);
			//if(ret==-1) printf("send failed %d\n",ret);
#endif
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "EXIST\r\n");
		} 	
		
		break;
	case KVS_CMD_GET: {
		char *result = kvs_array_get(&global_array, key);
		if (result == NULL) {
			length = sprintf(response, "NO EXIST\r\n");
		} else {
			length = sprintf(response, "%s\r\n", result);
		}
		break;
	}



	case KVS_CMD_DEL:
		ret = kvs_array_del(&global_array ,key);
		
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
if (ENABLE_AOF){
            kvs_aof_write("DEL",key,NULL);
}            
#if ENABLE_MS
			char send_buffer[BUFFER_LENGTH];
			int n=kvs_join_tokens(send_token,3,send_buffer);
			kvs_sync_msg(send_buffer,n);
			//printf("del ret:%d\n",ret);
#endif
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
	case KVS_CMD_MOD:
		ret = kvs_array_mod(&global_array ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
if (ENABLE_AOF){
            kvs_aof_write("MOD",key,value);
}
#if ENABLE_MS
			char send_buffer[BUFFER_LENGTH];
			int n=kvs_join_tokens(send_token,4,send_buffer);
			kvs_sync_msg(send_buffer,n);
#endif
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
	case KVS_CMD_EXIST:
		ret = kvs_array_exist(&global_array ,key);
		if (ret == 0) {
			length = sprintf(response, "EXIST\r\n");
		} else {
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
#endif
	// rbtree
#if ENABLE_RBTREE
	case KVS_CMD_RSET:
		ret = kvs_rbtree_set(&global_rbtree ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
		} else if (ret == 0) {
if (ENABLE_AOF){
            kvs_aof_write("RSET",key,value);
}
#if ENABLE_MS
			char send_buffer[BUFFER_LENGTH];
			int n=kvs_join_tokens(send_token,4,send_buffer);
			kvs_sync_msg(send_buffer,n);
#endif
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "EXIST\r\n");
		} 
		
		break;
	case KVS_CMD_RGET: {
		char *result = kvs_rbtree_get(&global_rbtree, key);
		if (result == NULL) {
			length = sprintf(response, "NO EXIST\r\n");
		} else {
			length = sprintf(response, "%s\r\n", result);
		}
		break;
	}
	case KVS_CMD_RDEL:
		ret = kvs_rbtree_del(&global_rbtree ,key);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
if (ENABLE_AOF){
            kvs_aof_write("RDEL",key,NULL);
}
#if ENABLE_MS
			char send_buffer[BUFFER_LENGTH];
			int n=kvs_join_tokens(send_token,3,send_buffer);
			kvs_sync_msg(send_buffer,n);
#endif
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
	case KVS_CMD_RMOD:
		ret = kvs_rbtree_mod(&global_rbtree ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
if (ENABLE_AOF){
            kvs_aof_write("MOD",key,value);
}
#if ENABLE_MS
			char send_buffer[BUFFER_LENGTH];
			int n=kvs_join_tokens(send_token,4,send_buffer);
			kvs_sync_msg(send_buffer,n);
#endif
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
	case KVS_CMD_REXIST:
		ret = kvs_rbtree_exist(&global_rbtree ,key);
		if (ret == 0) {
			length = sprintf(response, "EXIST\r\n");
		} else {
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
#endif
#if ENABLE_HASH
	case KVS_CMD_HSET:
		ret = kvs_hash_set(&global_hash ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
		} else if (ret == 0) {
if (ENABLE_AOF){
            kvs_aof_write("HSET",key,value);
}
#if ENABLE_MS
			char send_buffer[BUFFER_LENGTH];
			int n=kvs_join_tokens(send_token,4,send_buffer);
			kvs_sync_msg(send_buffer,n);
#endif
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "EXIST\r\n");
		} 
		
		break;
	case KVS_CMD_HGET: {
		char *result = kvs_hash_get(&global_hash, key);
		if (result == NULL) {
			length = sprintf(response, "NO EXIST\r\n");
		} else {
			length = sprintf(response, "%s\r\n", result);
		}
		break;
	}
	case KVS_CMD_HDEL:
		ret = kvs_hash_del(&global_hash ,key);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
if (ENABLE_AOF){
            kvs_aof_write("HDEL",key,NULL);
}
#if ENABLE_MS
			char send_buffer[BUFFER_LENGTH];
			int n=kvs_join_tokens(send_token,3,send_buffer);
			kvs_sync_msg(send_buffer,n);
#endif
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
	case KVS_CMD_HMOD:
		ret = kvs_hash_mod(&global_hash ,key, value);
		if (ret < 0) {
			length = sprintf(response, "ERROR\r\n");
 		} else if (ret == 0) {
if (ENABLE_AOF){
            kvs_aof_write("HMOD",key,value);
}
#if ENABLE_MS
			char send_buffer[BUFFER_LENGTH];
			kvs_join_tokens(send_token,4,send_buffer);
			kvs_sync_msg(send_buffer,strlen(send_buffer));
#endif
			length = sprintf(response, "OK\r\n");
		} else {
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
	case KVS_CMD_HEXIST:
		ret = kvs_hash_exist(&global_hash ,key);
		if (ret == 0) {
			length = sprintf(response, "EXIST\r\n");
		} else {
			length = sprintf(response, "NO EXIST\r\n");
		}
		break;
#endif
#if ENABLE_DATA
    case KVS_CMD_LSET:
        if (!key || !value) {
            length = sprintf(response, "ERROR\r\n");
            break;
        }
		//printf("%s %s\n",key,value);
        ret = kvs_large_set(key, value);
        if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }  else if(ret>0){
			length=sprintf(response,"EXIST\r\n");
		}
		else {
if (ENABLE_AOF){
            kvs_aof_write("LSET", key, value);
}
#if ENABLE_MS
			char send_buffer[BUFFER_LENGTH];
			kvs_join_tokens(send_token,4,send_buffer);
			kvs_sync_msg(send_buffer,strlen(send_buffer));
#endif
            length = sprintf(response, "OK\r\n");
        }
        break;

    case KVS_CMD_LGET: {
        if (!key) {
            length = sprintf(response, "ERROR\r\n");
            break;
        }

        char *res = kvs_large_get(key);
        if (res == NULL) {
            length = sprintf(response, "NO EXIST\r\n");
        } else {
            length = sprintf(response, "%s\r\n", res);
            free(res);
        }
        break;
    }

    case KVS_CMD_LDEL:
        if (!key) {
            length = sprintf(response, "ERROR\r\n");
            break;
        }

        ret = kvs_large_del(key);
        if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        } else {
if (ENABLE_AOF){
            kvs_aof_write("LDEL", key, NULL);
}
#if ENABLE_MS
			char send_buffer[BUFFER_LENGTH];
			kvs_join_tokens(send_token,3,send_buffer);
			kvs_sync_msg(send_buffer,strlen(send_buffer));
#endif
            length = sprintf(response, "OK\r\n");
        }
        break;

    case KVS_CMD_LMOD:
        if (!key || !value) {
            length = sprintf(response, "ERROR\r\n");
            break;
        }

        ret = kvs_large_mod(key, value);
        if (ret < 0) {
            length = sprintf(response, "NO EXIST\r\n");
        } else {
if (ENABLE_AOF){
            kvs_aof_write("LMOD", key, value);
}
#if ENABLE_MS
			char send_buffer[BUFFER_LENGTH];
			kvs_join_tokens(send_token,4,send_buffer);
			kvs_sync_msg(send_buffer,strlen(send_buffer));
#endif
            length = sprintf(response, "OK\r\n");
        }
        break;

    case KVS_CMD_LEXIST:
        if (!key) {
            length = sprintf(response, "ERROR\r\n");
            break;
        }

        ret = kvs_large_exist(key);
        if (ret == 0) {
            length = sprintf(response, "EXIST\r\n");
        } else {
            length = sprintf(response, "NO EXIST\r\n");
        }
        break;

#endif
#if ENABLE_MS
	case KVS_CMD_SYNC:
		printf("get cmd SYNC \n");
if (NETWORK_SELECT == NETWORK_PROACTOR)
		kvs_rdb_broadcast_all(); 
else if (NETWORK_SELECT == NETWORK_REACTOR)
		kvs_rdb_broadcast_all();
else if (NETWORK_SELECT == NETWORK_NTYCO)
		kvs_rdb_broadcast_all(); 
		memset(response,0,sizeof(response));
		length=0;
		break;
#endif
	default: 
		printf("assert 0:%s %d\n",tokens[0],cmd);
		response=NULL;
		length=0;
		//assert(0);
	}
#if 0
if(ENABLE_RDB&&(cmd==KVS_CMD_SET||cmd==KVS_CMD_MOD||cmd==KVS_CMD_DEL
			||cmd==KVS_CMD_RSET||cmd==KVS_CMD_RMOD||cmd==KVS_CMD_RDEL
			||cmd==KVS_CMD_HSET||cmd==KVS_CMD_HMOD||cmd==KVS_CMD_HDEL)){
	 int rdb_ret = kvs_rdb_save();
        if (rdb_ret != 0) {
            printf("rdb save failed\n");
        } else {
            //printf("rdb save success\n");
        }
}
#endif

	return length;
}


/*
 * msg: request message
 * length: length of request message
 * response: need to send
 * @return : length of response
 */

int kvs_protocol(char *msg, int length, char *response) {  //
	
// SET Key Value
// GET Key
// DEL Key
	if (msg == NULL || length <= 0 || response == NULL) return -1;
	//printf("recv %d : %s\n", length, msg);
	int ans=0;
#if ENABLE_MS
#endif

#if ONE_MSG
	char *tokens[KVS_MAX_TOKENS] = {0};

	int count = kvs_split_token(msg, tokens);
	if (count == -1) return -1;

	//memcpy(response, msg, length);



	return kvs_filter_protocol(tokens, count, response);
#endif
#if MULTI_MSG
	//printf("start multi\n");
		// char *tmp=strtok(msg,"\n");
		// while(tmp!=NULL){
		// 	printf("tmp: %s\n msg:%s",tmp,msg);
		// 	ans=kvs_multi_start(tmp,strlen(tmp),response,MAX_RESP_LEN);
		// 	tmp=strtok(NULL,"\n");
		// }
	ans=kvs_multi_start(msg,strlen(msg),response,MAX_RESP_LEN);
	return ans;
#endif
}


extern void dest_kvengine(void);
int init_kvengine(void) {
#if MULTI_MSG
	

#endif
#if ENABLE_ARRAY
	memset(&global_array, 0, sizeof(kvs_array_t));
	kvs_array_create(&global_array);

#endif

#if ENABLE_RBTREE
	memset(&global_rbtree, 0, sizeof(kvs_rbtree_t));
	kvs_rbtree_create(&global_rbtree);
#endif

#if ENABLE_HASH
	memset(&global_hash, 0, sizeof(kvs_hash_t));
	kvs_hash_create(&global_hash);
#endif
if(kvs_role==ROLE_MASTER){
if (ENABLE_RDB){
    if((kvs_rdb_load())!=0){
        printf("rdb_load failed\n");
    }



}

if (ENABLE_AOF){
	//printf("start load aof\n");
	//char *file="data_files/kvs_aof.aof";
	kvs_aof_init();
    if(kvs_aof_load()!=0){
        printf("load aof file failed\n");
    }
}
}




    

}

void dest_kvengine(void) {
#if ENABLE_ARRAY
	kvs_array_destory(&global_array);
#endif
#if ENABLE_RBTREE
	kvs_rbtree_destory(&global_rbtree);
#endif
#if ENABLE_HASH
	kvs_hash_destory(&global_hash);
#endif

}


#if 0
int main(int argc, char *argv[]) {

	if (argc != 2) return -1;

	int port = atoi(argv[1]);

	init_kvengine();
	
	
#if (NETWORK_SELECT == NETWORK_REACTOR)
	reactor_start(port, kvs_protocol);  //
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
	ntyco_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
	proactor_start(port, kvs_protocol);
#endif
	dest_kvengine();

}

#endif

#if ENABLE_MS
int client_fds[MAX_CLIENTS];
int client_count=0;
char *reactor_response;
char *master_ip;
int master_fd;
int current_fd;
int master_port;
int slave_port;
kvs_role_t kvs_role;
//extern int kvs_ms_protocol(char *msg, int len,char*response);
int main(int argc, char*argv[]){
	load_config();
	int port;
	master_port =3000;	
	slave_port=master_port+1;
	if(argc<2){
		printf("Usage:\n");
        printf("  %s master [port]\n", argv[0]);
        printf("  %s slave <master_ip> [port]\n", argv[0]);
		return -1;
	}

	if(strcmp(argv[1],"master")==0){
		port = atoi(argv[2]);
		kvs_role=ROLE_MASTER;		
	}
	else if(strcmp(argv[1],"slave")==0){
		if(argc<3){
			printf("Usage: %s slave <master_ip> [port]\n", argv[0]);
            return -1;
		}
		port=atoi(argv[3]);
		master_ip=argv[2];
		kvs_role=ROLE_SLAVE;

	}else{
		printf("Unknown role: %s\n", argv[1]);
	}

	init_kvengine();
	if (ENABLE_RDB) {
		start_rdb_save_thread();
	}
if (NETWORK_SELECT == NETWORK_REACTOR)
	reactor_start(port, kvs_protocol);  //
else if (NETWORK_SELECT == NETWORK_PROACTOR)
	proactor_start(port, kvs_protocol);
else if (NETWORK_SELECT == NETWORK_NTYCO)
	
	ntyco_start(port, kvs_protocol);
	
	dest_kvengine();
if(ENABLE_AOF){
	kvs_aof_close();
}
	
}

#endif