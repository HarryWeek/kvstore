#include"kvstore.h"
#if MULTI_MSG

void kvs_trim_newline(char *s) {
    int len = strlen(s);
    while (len > 0 && (s[len - 1] == '\n' || s[len - 1] == '\r' || s[len - 1] == ' '))
        s[--len] = '\0';
}
kvs_multi_t *kvs_multi_create(){
    kvs_multi_t *s=kvs_malloc(sizeof(kvs_multi_t));
    if(!s) return NULL;
    memset(s,0,sizeof(kvs_multi_t));
    return s;    
}

void kvs_multi_destroy(kvs_multi_t *s){
    if(!s) return ;
    //printf("s->count:%d\n",s->count);
    // for(int i=0;i<s->count;i++){
    //     kvs_free(s->queue[i]);
    // }
    kvs_free(s);
}

#if 0
int kvs_multi_response(char *msg,int length,char* response,kvs_multi_t *multi_msg,int max_resp_len){
	if(!msg||length<=0) return -1;
    
	if(strcmp(msg,"MULTI")==0){
        if(multi_msg->int_tx){
            printf("ERROR: already in multi queue\n");
        }
		multi_msg->int_tx=1;
		multi_msg->count=0;
		printf("multi start\n");
	}else if(strcmp(msg,"EXEC")==0){
		printf("EXEC start\n");
        int total_written=0;
		for(int i=0;i<multi_msg->count;i++){
	        char *tokens[KVS_MAX_TOKENS] = {0};
            char ans[256]={0};
	        int count = kvs_split_token(multi_msg->queue[i], tokens);
	        if (count == -1) return -1;
#if ENABLE_MS
        if(strcmp(tokens[0],"SET")==0||strcmp(tokens[0],"MOD")==0||strcmp(tokens[0],"DEL")==0||
            strcmp(tokens[0],"RSET")==0||strcmp(tokens[0],"RMOD")==0||strcmp(tokens[0],"RDEL")==0||
            strcmp(tokens[0],"HSET")==0||strcmp(tokens[0],"HMOD")==0||strcmp(tokens[0],"HDEL")==0||
            strcmp(tokens[0],"LSET")==0||strcmp(tokens[0],"LMOD")==0||strcmp(tokens[0],"LDEL")==0){
                kvs_join_tokens(tokens,3,msg);
                proactor_broadcast(msg,strlen(msg));
            }
#endif
	        //memcpy(response, msg, length);
            printf("command: %s\n",multi_msg->queue[i]);
	        int multi_ret= kvs_filter_protocol(tokens, count, ans); 
            int n=snprintf(response+total_written,
                            max_resp_len-total_written,
                            "%s",ans);
            total_written+=n;
            if(total_written>=max_resp_len) break;
            //printf("response: %s\n",response);       
            //return multi_ret;    
            //kvs_free(multi_msg->queue[i]);
        
		}
        multi_msg->count=0;
        multi_msg->int_tx=0;
        printf("EXEC completed\n");
        //return 0;
	}else if(strcmp(msg,"DISCARD")==0){
        if(multi_msg->int_tx==0){
            printf("ERROR: not in multi queue\n");
            return -1;
        }
        // for(int i=0;i<multi_msg->count;i++){
        //     kvs_free(multi_msg->queue[i]);
        // }
        multi_msg->int_tx=0;
        multi_msg->count=0;
        //printf("")
    }else if(multi_msg->int_tx){
        if(multi_msg->count>=MAX_MSG){
            printf("error: queue is full\n");
            return -1;
        }
        multi_msg->queue[multi_msg->count++]=strdup(msg);
        printf("command %s added in queue\n",msg);
    }else{


        char *tokens[KVS_MAX_TOKENS] = {0};
        int count=0;
        count=kvs_split_token(msg,tokens);
        //printf("token 0:%s\n",tokens[0]);
        //printf("msg:%s\n",msg);
#if ENABLE_MS
        char tmp[1024]={0};
        if(strcmp(tokens[0],"SYNC")==0){
            printf("get SYNC\n");
        }
        if(strcmp(tokens[0],"SET")==0||strcmp(tokens[0],"MOD")==0||strcmp(tokens[0],"DEL")==0||
            strcmp(tokens[0],"RSET")==0||strcmp(tokens[0],"RMOD")==0||strcmp(tokens[0],"RDEL")==0||
            strcmp(tokens[0],"HSET")==0||strcmp(tokens[0],"HMOD")==0||strcmp(tokens[0],"HDEL")==0||
            strcmp(tokens[0],"LSET")==0||strcmp(tokens[0],"LMOD")==0||strcmp(tokens[0],"LDEL")==0){
                kvs_join_tokens(tokens,3,tmp);
                //printf("tmp:%s\n",tmp);
                proactor_broadcast(tmp,strlen(tmp));
                //if (count == -1) return -1;

                //memcpy(response, msg, length);
                //printf("tokens[0]: %s msg:%s\n",tokens[0],msg);
                
            }else
        if(strcmp(tokens[0],"SSET")==0||strcmp(tokens[0],"SMOD")==0||strcmp(tokens[0],"SDEL")==0||
            strcmp(tokens[0],"SRSET")==0||strcmp(tokens[0],"SRMOD")==0||strcmp(tokens[0],"SRDEL")==0||
            strcmp(tokens[0],"SHSET")==0||strcmp(tokens[0],"SHMOD")==0||strcmp(tokens[0],"SHDEL")==0||
            strcmp(tokens[0],"SLSET")==0||strcmp(tokens[0],"SLMOD")==0||strcmp(tokens[0],"SLDEL")==0||strcmp(tokens[0],"SSYNC")==0){
                printf("get SYNC cmd:%s %s %s\n",tokens[0],tokens[1],tokens[2]);
                int ans=kvs_ms_filter_protocol(tokens,count);
                //printf("ans: %d\n",ans);
                return ans;
            }
#endif
            return kvs_filter_protocol(tokens, count, response);





    }
    return 0;
}
#endif
extern void print_visible(char *msg);
int kvs_multi_start(char *msg,int length,char* response,int max_resp_len){
    //printf("all msg:");
    //print_visible(msg);
    
    //printf("\nstart response: %s\n", response);

	

    int len = 0;  // response 当前长度
    char *p=msg;
    //printf("first char: [%c] (%d)\n", *msg, *msg);
    while (*p != '\0') {
        char *resp = kvs_malloc(1024);
        char *tokens[KVS_MAX_TOKENS] = {0};

        // 解析一条 RESP 命令
        int msg_len = kvs_split_token(p, tokens);
        if (msg_len <= 0) break;  // 没有更多命令或格式错误

        //printf("%s %s %s %s\n",tokens[0],tokens[1],tokens[2],tokens[3]);
        // 执行命令
        if(strcmp(tokens[0],"SYNC")==0){
            //printf("%s %s %s %s\n",tokens[0],tokens[1],tokens[2],tokens[3]);
            kvs_ms_filter_protocol(tokens,4);
            //len=sprintf(response,"SYNCC completed\r\n");
            len=0;
            //response=NULL;
        }else{
            kvs_filter_protocol(tokens, 3, resp);
            // 拼接响应结果（追加模式）
            //printf("\nresp:");
            //print_visible(resp);
            len += sprintf(response + len, "%s", resp);
        }
        



        // 前进到下一条命令
        p += msg_len;

        kvs_free(resp);
        for (int i = 0; tokens[i]; i++) free(tokens[i]); // 如果malloc的就释放
    }
    //printf("reactor_response %s\n",reactor_response);
    //response=reactor_response;
    //len=strlen(response);
    response[len] = '\0';
    //print_visible(response);
    //printf("Final response: %s\n", response);
    // while(*response!='*'){
    //     response++;
    //     len--;
    // }
    return len;
}
#endif