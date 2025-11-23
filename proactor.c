

#include"kvstore.h"
#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define EVENT_ACCEPT   	0
#include <netinet/tcp.h>
#include <fcntl.h>
#define EVENT_READ		1
#define EVENT_WRITE		2
#define TYPE_DATA       0
#define TYPE_ACK        1
#define ENTRIES_LENGTH		1024
#define BUFFER_LENGTH		1024
#define MAX_CONN 65535
#define EXPIRE_TIME 60000  // 重传超时时间，单位毫秒
extern int kvs_protocol(char *msg, int length, char *response);
extern int kvs_ms_protocol(char *msg, int len,char*response);
void check_retransmit(int fd);
#if ENABLE_MS

extern int client_fds[MAX_CLIENTS];
extern int client_count;
extern int master_port;
extern int slave_port;
extern struct io_uring ring;  // 引用主loop的全局ring

typedef struct Node{
    char *msg;
    int msg_id;
    uint64_t expire_at;  // 超时时间点（毫秒）
    int retry_count;
    struct Node* next;
}Node;

typedef struct ack_msg_s {
    char *msg;
    struct ack_msg_s *next;
} ack_msg_t;

typedef struct connection_s{
    int fd;
    char rbuffer[BUFFER_LENGTH];
    int rlength;
    char wbuffer[BUFFER_LENGTH];
    int wlength;
    int recv_pending;
    Node *send_queue_head;//存放待发送但未确认的数据包
    Node *send_queue_tail;
    int queue_size;
    int msg_id;
    int retransmit_count;//已返回确认帧的序号，该序号之前的数据包都已被确认，新序号的差大于1则需要重传
    ack_msg_t *ack_queue_head; // 待发送的ack消息队列
    ack_msg_t *ack_queue_tail;
} connection_t;

connection_t p_conn_list[MAX_CONN];



int set_event_send(struct io_uring *ring, int sockfd,
				      void *buf, size_t len, int flags);
int set_event_recv(struct io_uring *ring, int sockfd,
				      void *buf, size_t len, int flags);
static void add_client_fd(int fd) {
	for(int i=0;i<client_count;i++){
		if(client_fds[i]==fd) return;
	}
    if (client_count < MAX_CLIENTS) {
        client_fds[client_count++] = fd;
    }
}

static void remove_client_fd(int fd) {
    for (int i = 0; i < client_count; i++) {
        if (client_fds[i] == fd) {
            client_fds[i] = client_fds[client_count - 1];
            client_count--;
            break;
        }
    }
}
void print_visible(char *msg) {
    for (char *p = msg; *p; p++) {
        if (*p == '\r') {
            printf("\\r");
        } else if (*p == '\n') {
            printf("\\n");
        } else {
            putchar(*p);
        }
    }
}
uint64_t now_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}
char *pack_header(char* msg,int len,int msg_id,int type){
    char* p=kvs_malloc(BUFFER_LENGTH);
    int p_len=sprintf(p,"@%d^%d\r\n%s",msg_id,type,msg);
    p[p_len]='\0';
    return p;
}
int unpack_header(char* msg,int* msg_id,int* type,int *head_len){
    if(msg[0]!='@') return -1;
    char* p=msg+1;
    *msg_id=atoi(p);
    char* caret=strchr(p,'^');
    if(!caret) return -1;
    p=caret+1;
    *type=atoi(p);
    char* rn=strstr(p,"\r\n");
    if(!rn) return -1;
    p=rn+2;
    *head_len=p-msg;
    return 0;
}

// 检查消息是否为ack消息（格式：@数字^1\r\n）
static int is_ack_message(const char *msg) {
    if(!msg || msg[0] != '@') return 0;
    char *caret = strchr(msg, '^');
    if(!caret) return 0;
    int type = atoi(caret + 1);
    return (type == TYPE_ACK);
}

int proactor_broadcast( char *msg, size_t len) {
	//printf("client_count:%d\n",client_count);
    if (client_count == 0) return -1;

    for (int i = 0; i < client_count; i++) {
        int fd = client_fds[i];
		//char *send_bufer=msg;
		//sprintf(send_buffer,"S%s\r\n",msg);
        char *packet=pack_header(msg,len,p_conn_list[fd].msg_id,TYPE_DATA);
        p_conn_list[fd].msg_id++;
        connection_t *c=&p_conn_list[fd];
        if(c->send_queue_head==NULL){
            c->send_queue_head=(Node*)kvs_malloc(sizeof(Node));
            c->send_queue_head->msg=packet;
            c->send_queue_head->next=NULL;
            c->send_queue_head->expire_at=now_ms()+EXPIRE_TIME;
            c->send_queue_head->retry_count=0;
            c->send_queue_tail=c->send_queue_head;
            c->send_queue_head->msg_id=p_conn_list[fd].msg_id -1;
            //c->msg_id++;
            c->queue_size=1;
        }else{
            c->send_queue_tail->next=(Node*)kvs_malloc(sizeof(Node));
            c->send_queue_tail=c->send_queue_tail->next;
            c->send_queue_tail->msg=packet;
            c->send_queue_tail->next=NULL;
            c->send_queue_tail->expire_at=now_ms()+EXPIRE_TIME;
            c->send_queue_tail->retry_count=0;
            c->send_queue_tail->msg_id=p_conn_list[fd].msg_id -1;
            //c->msg_id++;
            c->queue_size++;
        }
        set_event_send(&ring, fd,packet, strlen(packet), 0);
		// printf("send: ");
		// print_visible(packet);
		// printf(" to fd:%d i:%d\n", fd, i);
    }

    // 提交所有发送任务
    io_uring_submit(&ring);
    return client_count;
}
int proactor_connect(char *master_ip, unsigned short conn_port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }

    struct sockaddr_in serveraddr;
    memset(&serveraddr, 0, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_port = htons(conn_port);

    if (inet_pton(AF_INET, master_ip, &serveraddr.sin_addr) <= 0) {
        perror("inet_pton");
        close(sockfd);
        return -1;
    }

    if (connect(sockfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) < 0) {
        perror("connect");
        close(sockfd);
        return -1;
    }

    // 建立成功，加入客户端列表
    // Disable Nagle and set non-blocking on the outbound connection
    {
        int nagle_off = 1;
        setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &nagle_off, sizeof(nagle_off));
        int f = fcntl(sockfd, F_GETFL, 0);
        if (f >= 0) fcntl(sockfd, F_SETFL, f | O_NONBLOCK);
    }
    add_client_fd(sockfd);
    // 初始化连接结构体
    connection_t *conn = &p_conn_list[sockfd];
    conn->fd = sockfd;
    conn->rlength = 0;
    conn->wlength = 0;
    conn->recv_pending = 0;
    conn->msg_id = 1;
    conn->retransmit_count = 0;
    conn->send_queue_head = NULL;
    conn->send_queue_tail = NULL;
    conn->queue_size = 0;
    conn->ack_queue_head = NULL;
    conn->ack_queue_tail = NULL;
    memset(conn->rbuffer, 0, BUFFER_LENGTH);
    memset(conn->wbuffer, 0, BUFFER_LENGTH);
    // 注册recv事件，等主节点发数据
    if (sockfd < 0 || sockfd >= MAX_CONN) {
        close(sockfd);
        return -1;
    }
    // if (set_event_recv(&ring, sockfd, conn->rbuffer, BUFFER_LENGTH, 0) < 0) {
    //     fprintf(stderr, "[proactor_connect] set_event_recv failed for fd %d\n", sockfd);
    //     close(sockfd);
    //     return -1;
    // }
    // conn->recv_pending = 1;
    // io_uring_submit(&ring);

    printf("[proactor] Connected to master %s:%d (fd=%d)\n", master_ip, conn_port, sockfd);

    return sockfd;
}

#endif


struct conn_info {
	int fd;
	int event;
};


int p_init_server(unsigned short port) {	

	int sockfd = socket(AF_INET, SOCK_STREAM, 0);	
	struct sockaddr_in serveraddr;	
	memset(&serveraddr, 0, sizeof(struct sockaddr_in));	
	serveraddr.sin_family = AF_INET;	
	serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);	
	serveraddr.sin_port = htons(port);	

	if (-1 == bind(sockfd, (struct sockaddr*)&serveraddr, sizeof(struct sockaddr))) {		
		perror("bind");		
		return -1;	
	}	

	listen(sockfd, 10);
	
	return sockfd;
}





int set_event_recv(struct io_uring *ring, int sockfd,
				      void *buf, size_t len, int flags) {

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        fprintf(stderr, "[set_event_recv] failed to get SQE: submission queue full\n");
        return -1;
    }

    struct conn_info accept_info = {
        .fd = sockfd,
        .event = EVENT_READ,
    };
	
    io_uring_prep_recv(sqe, sockfd, buf, len, flags);
    memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));
    return 0;

}


int set_event_send(struct io_uring *ring, int sockfd,
				      void *buf, size_t len, int flags) {
	
	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        fprintf(stderr, "[set_event_send] failed to get SQE: submission queue full\n");
        return -1;
    }
	struct conn_info accept_info = {
		.fd = sockfd,
		.event = EVENT_WRITE,
	};
	
	io_uring_prep_send(sqe, sockfd, buf, len, flags);
    io_uring_sqe_set_flags(sqe, IOSQE_ASYNC);
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));
	// int ret = io_uring_submit(ring);
    // if (ret < 0) {
    //     fprintf(stderr, "[set_event_send] io_uring_submit() failed: %s\n", strerror(-ret));
    //     return -1;
    // } else if (ret == 0) {
    //     fprintf(stderr, "[set_event_send] io_uring_submit() returned 0, no SQE submitted\n");
    //     return -1;
    // }
	return 0;
}



int set_event_accept(struct io_uring *ring, int sockfd, struct sockaddr *addr,
					socklen_t *addrlen, int flags) {

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        fprintf(stderr, "[set_event_accept] failed to get SQE: submission queue full\n");
        return -1;
    }

    struct conn_info accept_info = {
        .fd = sockfd,
        .event = EVENT_ACCEPT,
    };
	
    io_uring_prep_accept(sqe, sockfd, (struct sockaddr*)addr, addrlen, flags);
    memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));
    return 0;

}

void handler_ack(connection_t *conn,int msg_id,int fd){
    Node *curr=conn->send_queue_head;
    Node *prev=NULL;
    while(curr!=NULL){
        if(curr->msg_id==msg_id){
            //仅删除该节点
            if(prev==NULL){//删除头节点
                conn->send_queue_head=curr->next;
                if(conn->send_queue_head==NULL){
                    conn->send_queue_tail=NULL;
                }
            }else{
                prev->next=curr->next;
                if(curr==conn->send_queue_tail){
                    conn->send_queue_tail=prev;
                }
            }
            kvs_free(curr->msg);
            kvs_free(curr);
            conn->queue_size--;
            break;
        }else{
            prev=curr;
            curr=curr->next;
        }
    }
    //check_retransmit(fd);
}

void check_retransmit(int fd){
    for(int i=0;i<client_count;i++){
        if(client_fds[i]==fd){
            break;
        }
        if(i==client_count-1){
            return; //fd不在客户端列表中
        }
    }
    connection_t *conn=&p_conn_list[fd];
    uint64_t now=now_ms();
    Node *curr=conn->send_queue_head;
    while(curr!=NULL){
        if(now>=curr->expire_at){
            //超时，重传
            set_event_send(&ring, fd, curr->msg, strlen(curr->msg), 0);
            io_uring_submit(&ring);
            curr->expire_at=now+EXPIRE_TIME*(curr->retry_count+1); //指数退避
            curr->retry_count++;
            printf("[proactor] retransmit msg_id=%d to fd=%d, retry_count=%d\n", curr->msg_id, fd, curr->retry_count);
        }
        curr=curr->next;
    }
}

extern char syncc[1024];
typedef int (*msg_handler)(char *msg, int length, char *response);
static msg_handler kvs_handler;



struct io_uring ring;
int proactor_start(unsigned short port, msg_handler handler) {
	//printf("proactor start\n");
	int sockfd = p_init_server(port);
    //初始化连接结构体数组
    connection_t *conn = &p_conn_list[sockfd];
    conn->fd=sockfd;
    conn->rlength=0;
    conn->wlength=0;
    conn->recv_pending=0;
    conn->msg_id=1;
    conn->retransmit_count=0;
    conn->send_queue_head = NULL;
    conn->send_queue_tail = NULL;
    conn->queue_size = 0;
    conn->ack_queue_head = NULL;
    conn->ack_queue_tail = NULL;
    memset(conn->rbuffer,0,BUFFER_LENGTH);
    memset(conn->wbuffer,0,BUFFER_LENGTH);

	printf("proactor listen port: %d\n",port);
	kvs_handler = handler;
    char *tokens[] = {"SYNCALL"};
    kvs_join_tokens(tokens, 1, syncc);
	struct io_uring_params params;
	memset(&params, 0, sizeof(params));

    // 添加详细的错误检查
    int ret = io_uring_queue_init_params(ENTRIES_LENGTH, &ring, &params);
    if (ret < 0) {
        fprintf(stderr, "io_uring_queue_init_params failed: %s\n", strerror(-ret));
        close(sockfd);
        return -1;
    }
    printf("io_uring initialized successfully\n");
    
    struct sockaddr_in clientaddr;    
    socklen_t len = sizeof(clientaddr);
    
    // 检查设置accept事件是否成功
    if (set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0) < 0) {
        fprintf(stderr, "set_event_accept failed\n");
        io_uring_queue_exit(&ring);
        close(sockfd);
        return -1;
    }
    printf("Accept event set successfully\n");

	char buffer[BUFFER_LENGTH] = {0};
	char response[BUFFER_LENGTH] = {0};
#if ENABLE_MS

	extern char *master_ip;
	//extern unsigned short master_port;
	if(kvs_role==ROLE_SLAVE){
		printf("[proactor] Trying to connect master %s:%d ...\n", master_ip, port);
		int master_fd = proactor_connect(master_ip, port);
		if (master_fd >= 0) {
			add_client_fd(master_fd);
			printf("[proactor] Connected to master (fd=%d)\n", master_fd);
			
			//proactor_broadcast(sync,strlen(sync));
			//kvs_sync_msg(syncc,strlen(syncc));
            set_event_send(&ring, master_fd, syncc, strlen(syncc), 0);
		} else {
			fprintf(stderr, "[proactor] Failed to connect master %s:%d\n", master_ip, port);
		}
		//proactor_broadcast("SYNC",4);
	}

#endif
    int old_time=now_ms();
	while (1) {

		io_uring_submit(&ring);

        int curr_ms=now_ms();
        if(curr_ms-old_time>=1000){
            //每秒检查一次重传
            //printf("[proactor] Checking retransmissions...\n");
            old_time=curr_ms;;
            for(int i=0;i<client_count;i++){
                int fd=client_fds[i];
                check_retransmit(fd);
            }
        }
		//struct io_uring_cqe *cqe;
		//io_uring_wait_cqe(&ring, &cqe);

		struct io_uring_cqe *cqes[128];
		int nready = io_uring_peek_batch_cqe(&ring, cqes, 128);  // epoll_wait
        
		int i = 0;
		for (i = 0;i < nready;i ++) {

			struct io_uring_cqe *entries = cqes[i];
			struct conn_info result;
			memcpy(&result, &entries->user_data, sizeof(struct conn_info));
            
			if (result.event == EVENT_ACCEPT) {

                if (set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0) < 0) {
                    fprintf(stderr, "[proactor_start] set_event_accept failed\n");
                }
				//printf("set_event_accept\n"); //

                int connfd = entries->res;
                if (connfd < 0) {
                    // accept returned error
                    continue;
                }
                // disable Nagle on the accepted socket and make it non-blocking
                {
                    int nagle_off = 1;
                    setsockopt(connfd, IPPROTO_TCP, TCP_NODELAY, &nagle_off, sizeof(nagle_off));
                    int f = fcntl(connfd, F_GETFL, 0);
                    if (f >= 0) fcntl(connfd, F_SETFL, f | O_NONBLOCK);
                }
                if (connfd >= MAX_CONN) {
                    close(connfd);
                    continue;
                }
                // 初始化连接结构体数组
                connection_t *conn = &p_conn_list[connfd];
                conn->fd = connfd;
                conn->rlength = 0;
                conn->wlength = 0;
                conn->recv_pending = 0;
                conn->msg_id = 1;
                conn->retransmit_count = 0;
                conn->send_queue_head = NULL;
                conn->send_queue_tail = NULL;
                conn->queue_size = 0;
                conn->ack_queue_head = NULL;
                conn->ack_queue_tail = NULL;
                memset(conn->rbuffer, 0, BUFFER_LENGTH);
                memset(conn->wbuffer, 0, BUFFER_LENGTH);
                if (!conn->recv_pending) {
                    if (set_event_recv(&ring, connfd, conn->rbuffer + conn->rlength, BUFFER_LENGTH - conn->rlength, 0) < 0) {
                    fprintf(stderr, "[proactor_start] set_event_recv failed for fd %d\n", connfd);
                    close(connfd);
                    continue;
                }
                    conn->recv_pending = 1;
                }

				
			} else if (result.event == EVENT_READ) {  //

				int ret = entries->res;

				if (ret == 0) {
                    // remote closed
                    if (result.fd >=0 && result.fd < MAX_CONN) {
                        p_conn_list[result.fd].recv_pending = 0;
                    }
                    close(result.fd);

				} else if (ret > 0) {
#if 1
                    //int current_time=now_ms();

                    connection_t *conn2 = &p_conn_list[result.fd];
                    int fd=result.fd;
                    int ms_flag=0;
                    for(int i=0;i<client_count;i++){
                        if(client_fds[i]==fd){
                            ms_flag=1;
                            break;
                        }
                    }
                    if(ms_flag==0){
    #if ENABLE_MS
                        // 判断是否为 SYNC 消息（比较整个当前缓冲区内容）
                        connection_t *conn = &p_conn_list[result.fd];
    #endif
                        // 处理正常读取：数据已经被写入到 conn->rbuffer + old_rlength
                        if (result.fd < 0 || result.fd >= MAX_CONN) {
                            close(result.fd);
                            continue;
                        }

                        // 清除 recv_pending（当前 cqe 表示先前提交的 recv 已完成）
                        connection_t *conn2 = &p_conn_list[result.fd];
                        conn2->recv_pending = 0;

                        // 更新已接收长度
                        // 防止越界并计算加入前后的长度
                        size_t add = (size_t)ret;
                        if (conn2->rlength + add > BUFFER_LENGTH) {
                            add = BUFFER_LENGTH - conn2->rlength;
                            printf("[proactor] Warning: buffer overflow for fd %d, truncating data\n", result.fd);
                        }
                        int before_len = conn2->rlength + add; // 包含新数据的总长度
                        conn2->rlength += add;

    #if ENABLE_MS
                        if (conn->rlength == (int)strlen(syncc) && memcmp(conn->rbuffer, syncc, conn->rlength) == 0) {
                            printf("get SYNC fd:%d\n", result.fd);
                            add_client_fd(result.fd);
                        }
    #endif

                        //printf("get msg:%s\n", conn2->rbuffer);
                        // 如果缓冲区不以 '#' 开头，打印十六进制帮助诊断（协议应以 # 开头）
                        // if (conn2->rlength > 0 && conn2->rbuffer[0] != '#') {
                        //     fprintf(stderr, "[proactor] protocol error: buffer does not start with '#', fd=%d, rlength=%d\n", result.fd, conn2->rlength);
                        //     // 打印前 64 字节的 hex 视图
                        //     int dump = conn2->rlength < 64 ? conn2->rlength : 64;
                        //     for (int z = 0; z < dump; z++) {
                        //         fprintf(stderr, "%02X ", (unsigned char)conn2->rbuffer[z]);
                        //     }
                        //     fprintf(stderr, "\n");
                        // }

                        char *packet = parse_packet(conn2->rbuffer, &conn2->rlength, BUFFER_LENGTH);
                        if (!packet) {
                            // parse_packet 返回 NULL 可能表示：
                            //  - 不完整包（保留在缓冲区，等待后续数据）
                            //  - 协议错误（parse_packet 会将 *msg_len 置为 0）
                            if (conn2->rlength == 0) {
                                // 协议错误：记录并关闭连接
                                fprintf(stderr, "[proactor] protocol error, closing fd %d, buf='%s'\n", result.fd, conn2->rbuffer);
                                close(result.fd);
                                conn2->recv_pending = 0;
                            } else {
                                printf("remain buffer:%s\n",conn2->rbuffer);
                                // 不完整包：什么也不做，等待更多数据（下面会重新注册 recv）
                                // 可选：打印调试信息但不要退出进程
                                // fprintf(stderr, "[proactor] incomplete packet, waiting for more data, fd=%d, rlength=%d\n", result.fd, conn2->rlength);
                            }
                        } else {
                            int packet_len = before_len - conn2->rlength;
                            ret = kvs_handler(packet, packet_len, response);
                            set_event_send(&ring, result.fd, response, ret, 0);
                            //kvs_free(packet);
                        }

                        // 立即为该 fd 重新注册 recv（使用当前剩余空间），避免只在 write 完成后才重置
                        size_t avail_after = BUFFER_LENGTH - conn2->rlength;
                        if (avail_after > 0) {
                            if (!conn2->recv_pending) {
                                if (set_event_recv(&ring, result.fd, conn2->rbuffer + conn2->rlength, avail_after, 0) < 0) {
                                    fprintf(stderr, "[proactor] set_event_recv failed for fd %d after read\n", result.fd);
                                    close(result.fd);
                                    continue;
                                }
                                conn2->recv_pending = 1;
                            }
                        }
                    }else if(ms_flag==1){
                        char *buffer=conn2->rbuffer;
                        size_t add=ret;
                        if(conn2->rlength+add>BUFFER_LENGTH){
                            add=BUFFER_LENGTH - conn2->rlength;
                            printf("[proactor] Warning: buffer overflow for fd %d, truncating data\n", result.fd);
                        }
                        int before_len=conn2->rlength+add; //包含新数据的总长度
                        conn2->rlength+=add;
                        int *rlen=&conn2->rlength;
                        char packet[BUFFER_LENGTH];
                        int offset=0;
                        int pack_len=0;
                        // printf("get buffer:");
                        // print_visible(buffer);
                        // printf("\n");
                        while(1){
                            if(*rlen-offset<6) break;//缺少header
                            if(buffer[offset]!='@'){
                                char *p=memchr(buffer+offset,'@',*rlen - offset);//寻找header起始位置
                                if(p){
                                    int skip=p-(buffer+offset);
                                    memmove(buffer,buffer+offset+skip,*rlen-(offset+skip));
                                    offset=0;
                                    continue;
                                }else{
                                   // printf("[proactor] protocol error: missing '@' in header, fd=%d, rlength=%d\n", fd, *rlen);
                                    *rlen=0;
                                    printf("buffer:");
                                    print_visible(buffer);
                                    printf("\n");
                                    break;
                                }
                            }
                            int msg_id;
                            int type;
                            int head_len;
                            unpack_header(buffer+offset,&msg_id,&type,&head_len);//取出header
                            //memmove(buffer,buffer+offset+head_len,*rlen - (offset+head_len));
                            
                            //*rlen-=offset+head_len;
                            offset=0;
                            if(*rlen<=0) break;//没有数据体
                            if(type==TYPE_DATA){//处理数据帧
                                if(buffer[head_len]!='#'){
                                    //printf("[proactor] protocol error: data packet does not start with '#', fd=%d, rlength=%d\n", fd, *rlen);
                                    //数据帧数据缺失，丢弃该包，寻找下一个包
                                    char *p=memchr(buffer+head_len,'@',*rlen - head_len);//寻找header起始位置
                                    if(p){
                                        memmove(buffer,buffer+(p - buffer),*rlen - (p - buffer));
                                        *rlen=*rlen - (p - buffer);
                                        offset=0;
                                        continue;
                                    }else{
                                        //没有找到下一个包，清空缓冲区
                                        *rlen=0;
                                        break;
                                    }
                                }else{
                                    char *rn=memmem(buffer+head_len,*rlen,"\r\n",2);
                                    if(!rn){
                                        break; //数据体不完整，等待后续数据
                                    }
                                    int h_len=rn-(buffer+head_len)+2;
                                    char *pnum=buffer+head_len+1;
                                    char *pnum_end=rn;
                                    int body_len=0;
                                    if(pnum>=pnum_end){
                                        printf("[proactor] protocol error: empty data length, fd=%d\n", fd);
                                        //数据帧数据错误，丢弃该包，寻找下一个包
                                        continue;
                                    }
                                    for(char *t=pnum;t<pnum_end;t++){
                                        if(!isdigit((unsigned char)*t)){
                                            printf("[proactor] protocol error: invalid digit in data length, fd=%d\n", fd);
                                            //数据帧数据错误，丢弃该包，寻找下一个包
                                            continue;
                                        }
                                        body_len=body_len*10+(*t - '0');
                                        if(body_len<0){ //overflow check (defensive)
                                            printf("[proactor] protocol error: data length overflow, fd=%d\n", fd);
                                            //数据帧数据错误，丢弃该包，寻找下一个包
                                            continue;
                                        }
                                    }
                                    if(body_len<0){
                                        printf("[proactor] protocol error: negative data length, fd=%d\n", fd);
                                        //数据帧数据错误，丢弃该包，寻找下一个包
                                        continue;
                                    }
                                    if(body_len>*rlen - h_len - head_len){
                                        //数据体不完整，等待后续数据
                                        break;
                                    }
                                    char *p2=memchr(buffer+head_len+h_len,'@',*rlen - (head_len+h_len));
                                    if(p2){
                                        if(p2-(buffer+head_len)<body_len){
                                            //数据体错误，寻找下一个包
                                            memmove(buffer,p2,*rlen - (p2 - buffer));
                                            *rlen=*rlen - (p2 - buffer);
                                            offset=0;
                                            continue;
                                        }
                                    }
                                    //完整数据包，处理之
                                    memcpy(packet+pack_len,buffer+head_len,h_len+body_len);
                                    pack_len+=h_len+body_len;
                                    memmove(buffer,buffer+head_len+h_len+body_len,*rlen - (h_len+body_len+head_len));
                                    // printf("remove header:");
                                    // print_visible(buffer);
                                    // printf("\n");
                                    *rlen-=h_len+body_len+head_len;
                                    offset=0;
                                    // 将ack添加到队列，稍后批量发送
                                    char *ack_msg=pack_header("",0,msg_id,TYPE_ACK);
                                    ack_msg_t *ack_node = (ack_msg_t*)kvs_malloc(sizeof(ack_msg_t));
                                    ack_node->msg = ack_msg;
                                    ack_node->next = NULL;
                                    if(conn2->ack_queue_head == NULL){
                                        conn2->ack_queue_head = ack_node;
                                        conn2->ack_queue_tail = ack_node;
                                    }else{
                                        conn2->ack_queue_tail->next = ack_node;
                                        conn2->ack_queue_tail = ack_node;
                                    }
                                    //printf("[proactor] data packet received msg_id=%d from fd=%d, queued ack\n", msg_id, fd);
                                }

                            }else if(type==TYPE_ACK){//处理确认帧
                                handler_ack(conn2,msg_id,fd);
#if 0
                                Node *curr=conn2->send_queue_head;
                                memmove(buffer,buffer+head_len,*rlen - head_len);
                                *rlen-=head_len;
                                offset=0;
                                // while(curr->msg_id<conn2->retransmit_count){
                                //     //kvs_free(curr->msg);
                                //     curr=curr->next;
                                //     //kvs_free(conn2->send_queue_head);
                                //     conn2->send_queue_head=curr;
                                //     conn2->queue_size--;
                                    
                                // }
                                if(msg_id<=conn2->retransmit_count){
                                    //已经确认过的包，忽略
                                }else if(msg_id==conn2->retransmit_count+1){
                                    //正常确认，移除队列头
                                    if(curr){
                                        conn2->send_queue_head=curr->next;
                                        kvs_free(curr->msg);
                                        kvs_free(curr);
                                        conn2->queue_size--;
                                        conn2->retransmit_count++;
                                        printf("[proactor] ack received for msg_id=%d from fd=%d\n", conn2->retransmit_count, fd);
                                    }else{
                                        //没有待确认的数据包，可能是协议错误
                                        printf("[proactor] protocol error: no pending packets to ack, fd=%d\n", fd);
                                    }

                                }else{
                                    //需要重传多个包
                                    int to_retransmit=msg_id - (conn2->retransmit_count+1);
                                    printf("to_retransmit:%d\n",to_retransmit);
                                    for(int i=0;i<to_retransmit;i++){
                                        if(curr){
                                            //重传数据包
                                            //printf("[proactor] retransmit msg_id=%d to fd=%d\n", conn2->retransmit_count+i, fd);
                                            //print_visible(curr->msg);
                                            //printf("\n");
                                            // while(curr==NULL||curr->msg_id<conn2->retransmit_count+i){
                                            //     //kvs_free(curr->msg);
                                            //     Node* temp=curr;
                                            //     curr=curr->next;
                                            //     //kvs_free(temp);
                                            //     conn2->send_queue_head=curr;
                                            //     conn2->queue_size--;
                                            // }
                                            set_event_send(&ring,fd,curr->msg,strlen(curr->msg),0);
                                            printf("[proactor] retransmit packet msg_id=%d to fd=%d\n", conn2->retransmit_count+i, fd);
                                            io_uring_submit(&ring);
                                            conn2->send_queue_head=curr->next;
                                            conn2->queue_size--;
                                            //kvs_free(curr->msg);
                                            //kvs_free(curr);
                                            curr=curr->next;
                                            conn2->retransmit_count=curr->msg_id;
                                        }else{
                                            //没有足够的数据包，可能是协议错误
                                            printf("[proactor] protocol error: insufficient packets to ack, fd=%d\n", fd);
                                            break;
                                        }
                                    }
                                }
#endif
                            }


                        }
                        // 立即批量发送所有待发送的ack消息（在kvs_handler之前，避免被阻塞）
                        ack_msg_t *ack_curr = conn2->ack_queue_head;
                        while(ack_curr != NULL){
                            set_event_send(&ring, fd, ack_curr->msg, strlen(ack_curr->msg), 0);
                            ack_curr = ack_curr->next;
                        }
                        if(conn2->ack_queue_head != NULL){
                            io_uring_submit(&ring);  // 立即提交ack，不等待kvs_handler
                        }
                        ret=kvs_handler(packet,pack_len,response);
                        set_event_send(&ring, result.fd, response, ret, 0);
                        conn2->recv_pending=0;
                        //立即为该 fd 重新注册 recv（使用当前剩余空间），避免只在 write 完成后才重置
                        size_t avail_after = BUFFER_LENGTH - conn2->rlength;
                        //printf("after process remain len:%d\n",conn2->rlength);
                        conn2->rbuffer[conn2->rlength]='\0';
                        if(conn2->rlength>0){
                           // printf("remain buffer:");
                           // print_visible(conn2->rbuffer);
                           // printf("\n");
                        }
                        if (avail_after > 0) {
                            if (!conn2->recv_pending) {
                                if (set_event_recv(&ring, result.fd, conn2->rbuffer + conn2->rlength, avail_after, 0) < 0) {
                                    fprintf(stderr, "[proactor] set_event_recv failed for fd %d after read\n", result.fd);
                                    close(result.fd);
                                    continue;
                                }
                                conn2->recv_pending = 1;
                            }
                        }
                    }
                    

#endif
#if 0
#if ENABLE_MS
                    // 判断是否为 SYNC 消息（比较整个当前缓冲区内容）
                    connection_t *conn = &p_conn_list[result.fd];
#endif
                    // 处理正常读取：数据已经被写入到 conn->rbuffer + old_rlength
                    if (result.fd < 0 || result.fd >= MAX_CONN) {
                        close(result.fd);
                        continue;
                    }

                    // 清除 recv_pending（当前 cqe 表示先前提交的 recv 已完成）
                    connection_t *conn2 = &p_conn_list[result.fd];
                    conn2->recv_pending = 0;

                    // 更新已接收长度
                    // 防止越界并计算加入前后的长度
                    size_t add = (size_t)ret;
                    if (conn2->rlength + add > BUFFER_LENGTH) {
                        add = BUFFER_LENGTH - conn2->rlength;
                        printf("[proactor] Warning: buffer overflow for fd %d, truncating data\n", result.fd);
                    }
                    int before_len = conn2->rlength + add; // 包含新数据的总长度
                    conn2->rlength += add;

#if ENABLE_MS
                    if (conn->rlength == (int)strlen(syncc) && memcmp(conn->rbuffer, syncc, conn->rlength) == 0) {
                        printf("get SYNC fd:%d\n", result.fd);
                        add_client_fd(result.fd);
                    }
#endif

                    //printf("get msg:%s\n", conn2->rbuffer);
                    // 如果缓冲区不以 '#' 开头，打印十六进制帮助诊断（协议应以 # 开头）
                    // if (conn2->rlength > 0 && conn2->rbuffer[0] != '#') {
                    //     fprintf(stderr, "[proactor] protocol error: buffer does not start with '#', fd=%d, rlength=%d\n", result.fd, conn2->rlength);
                    //     // 打印前 64 字节的 hex 视图
                    //     int dump = conn2->rlength < 64 ? conn2->rlength : 64;
                    //     for (int z = 0; z < dump; z++) {
                    //         fprintf(stderr, "%02X ", (unsigned char)conn2->rbuffer[z]);
                    //     }
                    //     fprintf(stderr, "\n");
                    // }

                    char *packet = parse_packet(conn2->rbuffer, &conn2->rlength, BUFFER_LENGTH);
                    if (!packet) {
                        // parse_packet 返回 NULL 可能表示：
                        //  - 不完整包（保留在缓冲区，等待后续数据）
                        //  - 协议错误（parse_packet 会将 *msg_len 置为 0）
                        if (conn2->rlength == 0) {
                            // 协议错误：记录并关闭连接
                            fprintf(stderr, "[proactor] protocol error, closing fd %d, buf='%s'\n", result.fd, conn2->rbuffer);
                            close(result.fd);
                            conn2->recv_pending = 0;
                        } else {
                            printf("remain buffer:%s\n",conn2->rbuffer);
                            // 不完整包：什么也不做，等待更多数据（下面会重新注册 recv）
                            // 可选：打印调试信息但不要退出进程
                            // fprintf(stderr, "[proactor] incomplete packet, waiting for more data, fd=%d, rlength=%d\n", result.fd, conn2->rlength);
                        }
                    } else {
                        int packet_len = before_len - conn2->rlength;
                        ret = kvs_handler(packet, packet_len, response);
                        set_event_send(&ring, result.fd, response, ret, 0);
                        kvs_free(packet);
                    }

                    // 立即为该 fd 重新注册 recv（使用当前剩余空间），避免只在 write 完成后才重置
                    size_t avail_after = BUFFER_LENGTH - conn2->rlength;
                    if (avail_after > 0) {
                        if (!conn2->recv_pending) {
                            if (set_event_recv(&ring, result.fd, conn2->rbuffer + conn2->rlength, avail_after, 0) < 0) {
                                fprintf(stderr, "[proactor] set_event_recv failed for fd %d after read\n", result.fd);
                                close(result.fd);
                                continue;
                            }
                            conn2->recv_pending = 1;
                        }
                    }
#endif 

				}
			}  else if (result.event == EVENT_WRITE) {
  //

				int ret = entries->res;
				//printf("set_event_send ret: %d, %s\n", ret, buffer);

            if (result.fd < 0 || result.fd >= MAX_CONN) {
                close(result.fd);
                continue;
            }
            connection_t *conn3 = &p_conn_list[result.fd];
            // write 完成表示之前的 send 已完成，不影响 recv_pending
            if(ret<0){
                if(ret==-EPIPE||ret==-ECONNRESET){
                    // 连接被对端关闭
                    fprintf(stderr, "[proactor] write error on fd %d: %s, closing connection\n", result.fd, strerror(-ret));
                    close(result.fd);
                    remove_client_fd(result.fd);
                    continue;
                }else{
                    //fprintf(stderr, "[proactor] write error on fd %d: %s\n", result.fd, strerror(-ret));
                    continue;
                }
            }
            // 检查并释放已发送的ack消息（假设按FIFO顺序发送）
            // 注意：这是一个简化实现，实际应该通过user_data跟踪具体消息
            if(conn3->ack_queue_head != NULL && ret > 0){
                // 简单策略：每次write完成时释放队列头部的ack消息
                // 这假设ack消息是按顺序发送和完成的
                ack_msg_t *ack_to_free = conn3->ack_queue_head;
                conn3->ack_queue_head = ack_to_free->next;
                if(conn3->ack_queue_head == NULL){
                    conn3->ack_queue_tail = NULL;
                }
                kvs_free(ack_to_free->msg);
                kvs_free(ack_to_free);
            }
			size_t avail = BUFFER_LENGTH - conn3->rlength;
			if (avail == 0) {
                printf("[proactor] Warning: buffer full after write for fd %d, resetting rlength\n", result.fd);
				conn3->rlength = 0;
				avail = BUFFER_LENGTH;
			}
            if (!conn3->recv_pending) {
                if (set_event_recv(&ring, result.fd, conn3->rbuffer + conn3->rlength, avail, 0) < 0) {
                    fprintf(stderr, "[proactor_start] set_event_recv failed for fd %d after write\n", result.fd);
                    close(result.fd);
                    continue;
                }
                conn3->recv_pending = 1;
            }
				
			}
            
			
		}

		io_uring_cq_advance(&ring, nready);
        
	}

}


