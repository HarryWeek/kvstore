


#ifndef __KV_STORE_H__
#define __KV_STORE_H__


#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stddef.h>
#include <unistd.h>
#include <sys/types.h>
#include <pthread.h>
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <fcntl.h>
typedef enum {
    NETWORK_REACTOR = 0,
    NETWORK_PROACTOR = 1,
    NETWORK_NTYCO = 2
} network_type_t;
extern int g_network_mode;
#define NETWORK_SELECT		(g_network_mode)
int load_config();


#define KVS_MAX_TOKENS		128

#define ENABLE_ARRAY		1
#define ENABLE_RBTREE		1
#define ENABLE_HASH			1
extern int g_enable_rdb ;
extern int g_enable_aof ;
extern int ENABLE_RDB;
extern int ENABLE_AOF;
//#define ENABLE_RDB (g_enable_rdb)
//define ENABLE_AOF (g_enable_aof)

#define OLD_MALLOC 0
#define NEW_MALLOC 1

#define MULTI_MSG 1
#define ONE_MSG 0
#define MAX_MSG 128


#define ENABLE_MS 1
#define UN_ENABLE_MS 0


#define ENABLE_DATA 1

typedef int (*msg_handler)(char *msg, int length, char *response);


extern int reactor_start(unsigned short port, msg_handler handler);
extern int proactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);

int kvs_filter_protocol(char **tokens, int count, char *response);
int kvs_split_token(char *msg, char *tokens[]);
int kvs_join_tokens(char *tokens[], int count, char *msg);
char* parse_packet(char *msg, int *msg_len, int buffer_size); 
char* trans_parse_packet(char *msg, int *msg_len, int buffer_size);
#if ENABLE_ARRAY

typedef struct kvs_array_item_s {
	char *key;
	char *value;
} kvs_array_item_t;

#define KVS_ARRAY_SIZE		1000*10000

typedef struct kvs_array_s {
	kvs_array_item_t *table;
	int idx;
	int total;
} kvs_array_t;

int kvs_array_create(kvs_array_t *inst);
void kvs_array_destory(kvs_array_t *inst);

int kvs_array_set(kvs_array_t *inst, char *key, char *value);
char* kvs_array_get(kvs_array_t *inst, char *key);
int kvs_array_del(kvs_array_t *inst, char *key);
int kvs_array_mod(kvs_array_t *inst, char *key, char *value);
int kvs_array_exist(kvs_array_t *inst, char *key);

void kvs_array_print();

#endif


#if ENABLE_RBTREE

#define RED				1
#define BLACK 			2
#define TREE_LENGTH 1000*10000
#define ENABLE_KEY_CHAR		1

#if ENABLE_KEY_CHAR
typedef char* KEY_TYPE;
#else
typedef int KEY_TYPE; // key
#endif

typedef struct _rbtree_node {
	unsigned char color;
	struct _rbtree_node *right;
	struct _rbtree_node *left;
	struct _rbtree_node *parent;
	KEY_TYPE key;
	void *value;
} rbtree_node;

typedef struct _rbtree {
	rbtree_node *root;
	rbtree_node *nil;
} rbtree;


typedef struct _rbtree kvs_rbtree_t;

int kvs_rbtree_create(kvs_rbtree_t *inst);
void kvs_rbtree_destory(kvs_rbtree_t *inst);
int kvs_rbtree_set(kvs_rbtree_t *inst, char *key, char *value);
char* kvs_rbtree_get(kvs_rbtree_t *inst, char *key);
int kvs_rbtree_del(kvs_rbtree_t *inst, char *key);
int kvs_rbtree_mod(kvs_rbtree_t *inst, char *key, char *value);
int kvs_rbtree_exist(kvs_rbtree_t *inst, char *key);



#endif


#if ENABLE_HASH

#define MAX_KEY_LEN	64
#define MAX_VALUE_LEN	4096
#define MAX_TABLE_SIZE	1000*10000

#define ENABLE_KEY_POINTER	1


typedef struct hashnode_s {
#if ENABLE_KEY_POINTER
	char *key;
	char *value;
#else
	char key[MAX_KEY_LEN];
	char value[MAX_VALUE_LEN];
#endif
	struct hashnode_s *next;
	
} hashnode_t;


typedef struct hashtable_s {

	hashnode_t **nodes; //* change **, 

	int max_slots;
	int count;

} hashtable_t;

typedef struct hashtable_s kvs_hash_t;


int kvs_hash_create(kvs_hash_t *hash);
void kvs_hash_destory(kvs_hash_t *hash);
int kvs_hash_set(hashtable_t *hash, char *key, char *value);
char * kvs_hash_get(kvs_hash_t *hash, char *key);
int kvs_hash_mod(kvs_hash_t *hash, char *key, char *value);
int kvs_hash_del(kvs_hash_t *hash, char *key);
int kvs_hash_exist(kvs_hash_t *hash, char *key);


#endif



int kvs_rdb_save();
int kvs_array_save(char*filename,kvs_array_t *inst);
int kvs_rdb_load();
int kvs_array_load(char *filename,kvs_array_t *inst);
int kvs_rbtree_save(char*filename,rbtree *T);
int kvs_rbtree_load(char*filename,rbtree *T);
void *rdb_save_thread(void *arg);
int start_rdb_save_thread();
int kvs_hash_save(char* filename,kvs_hash_t *hash);

int kvs_hash_load(char* filename,kvs_hash_t *hash);
int kvs_rdb_broadcast_all();
int kvs_rdb_reactor_broadcast_all(char* response) ;




int kvs_aof_open(char *filename);
int kvs_aof_write(char *cmd,char *key,char *value);
void kvs_aof_close();
int kvs_aof_load();
int kvs_aof_init(void);
 void kvs_aof_flush(void);


#if OLD_MALLOC
void *kvs_malloc(size_t size);
void kvs_free(void *ptr);
#endif
#if NEW_MALLOC
void *kvs_malloc(size_t size);
void kvs_free(void*ptr);
void *je_calloc(size_t nmemb,size_t size);
void *je_realloc(void* ptr,size_t new_size);
#endif

#if MULTI_MSG
typedef struct kvs_multi_s{
	char* queue[MAX_MSG];//消息队列
	int count;//消息总数
	int int_tx;//是否进入消息队列
}kvs_multi_t;
int kvs_multi_start(char *msg,int length,char* response,int max_resp_len);
int kvs_multi_response(char *msg,int length,char* response,kvs_multi_t *multi_msg,int max_resp_len);
void kvs_multi_destroy(kvs_multi_t *s);
kvs_multi_t *kvs_multi_create();
#define MAX_RESP_LEN 1024
#endif

#if ENABLE_MS
typedef enum {
    ROLE_MASTER,
    ROLE_SLAVE
} kvs_role_t;

typedef struct {
    int fd;
    int active;
} slave_conn_t;

#define MAX_SLAVE 8

//extern kvs_role_t kvs_role;
extern char *reactor_response;
extern kvs_role_t kvs_role;
#define MAX_CLIENTS 16
int kvs_ms_protocol(char *msg, int len,char*response);
int proactor_broadcast(char *msg, size_t len);
int proactor_connect(char *master_ip, unsigned short master_port);
int kvs_ms_filter_protocol(char **tokens, int count);
int kvs_join_tokens(char *tokens[], int count, char *msg);
int reactor_broadcast(char *msg, size_t len);
int ntyco_broadcast(const char *msg, size_t len);
int kvs_sync_msg(char* msg,int len);
extern struct io_uring ring; 
int set_event_recv(struct io_uring *ring, int sockfd,
				      void *buf, size_t len, int flags);
#endif

#if ENABLE_DATA

#define VALUE_DATA_FILE  "data_files/value_data.db"
#define VALUE_INDEX_FILE "data_files/value_index.db"


typedef struct {
    char key[MAX_KEY_LEN];
    long offset;
    int length;
    int deleted;    // 0=有效, 1=删除
} kvs_large_index_t;
int kvs_large_set(char *key,  char *value);
char *kvs_large_get( char *key);
int kvs_large_del( char *key);
int kvs_large_mod( char *key,  char *value);
int kvs_large_exist( char *key);
#endif

#endif



