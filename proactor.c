

#include"kvstore.h"
#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define EVENT_ACCEPT   	0
#define EVENT_READ		1
#define EVENT_WRITE		2
#define ENTRIES_LENGTH		128
#define BUFFER_LENGTH		128
#define MAX_CONN 65535
extern int kvs_protocol(char *msg, int length, char *response);
extern int kvs_ms_protocol(char *msg, int len,char*response);
#if ENABLE_MS

extern int client_fds[MAX_CLIENTS];
extern int client_count;
extern int master_port;
extern int slave_port;
extern struct io_uring ring;  // 引用主loop的全局ring

typedef struct connection_s{
    int fd;
    char rbuffer[BUFFER_LENGTH];
    int rlength;
    char wbuffer[BUFFER_LENGTH];
    int wlength;
    int recv_pending;
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
int proactor_broadcast( char *msg, size_t len) {
	//printf("client_count:%d\n",client_count);
    if (client_count == 0) return -1;

    for (int i = 0; i < client_count; i++) {
        int fd = client_fds[i];
		//char *send_bufer=msg;
		//sprintf(send_buffer,"S%s\r\n",msg);
        set_event_send(&ring, fd,msg, strlen(msg), 0);
		//printf("send: ");
		//print_visible(msg);
		//printf(" to fd:%d i:%d\n", fd, i);
    }

    // 提交所有发送任务
    //io_uring_submit(&ring);
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
    add_client_fd(sockfd);
    // 初始化连接结构体
    connection_t *conn = &p_conn_list[sockfd];
    conn->fd = sockfd;
    conn->rlength = 0;
    conn->wlength = 0;
    conn->recv_pending = 0;
    memset(conn->rbuffer, 0, BUFFER_LENGTH);
    memset(conn->wbuffer, 0, BUFFER_LENGTH);
    // 注册recv事件，等主节点发数据
    if (sockfd < 0 || sockfd >= MAX_CONN) {
        close(sockfd);
        return -1;
    }
    if (set_event_recv(&ring, sockfd, conn->rbuffer, BUFFER_LENGTH, 0) < 0) {
        fprintf(stderr, "[proactor_connect] set_event_recv failed for fd %d\n", sockfd);
        close(sockfd);
        return -1;
    }
    conn->recv_pending = 1;
    io_uring_submit(&ring);

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
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));
	int ret = io_uring_submit(ring);
    if (ret < 0) {
        fprintf(stderr, "[set_event_send] io_uring_submit() failed: %s\n", strerror(-ret));
        return -1;
    } else if (ret == 0) {
        fprintf(stderr, "[set_event_send] io_uring_submit() returned 0, no SQE submitted\n");
        return -1;
    }
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
			kvs_sync_msg(syncc,strlen(syncc));
		} else {
			fprintf(stderr, "[proactor] Failed to connect master %s:%d\n", master_ip, port);
		}
		//proactor_broadcast("SYNC",4);
	}

#endif

	while (1) {

		io_uring_submit(&ring);


		struct io_uring_cqe *cqe;
		io_uring_wait_cqe(&ring, &cqe);

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
                    }
                    int before_len = conn2->rlength + add; // 包含新数据的总长度
                    conn2->rlength += add;

#if ENABLE_MS
                    if (conn->rlength == (int)strlen(syncc) && memcmp(conn->rbuffer, syncc, conn->rlength) == 0) {
                        printf("get SYNC fd:%d\n", result.fd);
                        add_client_fd(result.fd);
                    }
#endif

                    printf("get msg:%s\n", conn2->rbuffer);
                    // 如果缓冲区不以 '#' 开头，打印十六进制帮助诊断（协议应以 # 开头）
                    if (conn2->rlength > 0 && conn2->rbuffer[0] != '#') {
                        fprintf(stderr, "[proactor] protocol error: buffer does not start with '#', fd=%d, rlength=%d\n", result.fd, conn2->rlength);
                        // 打印前 64 字节的 hex 视图
                        int dump = conn2->rlength < 64 ? conn2->rlength : 64;
                        for (int z = 0; z < dump; z++) {
                            fprintf(stderr, "%02X ", (unsigned char)conn2->rbuffer[z]);
                        }
                        fprintf(stderr, "\n");
                    }

                    char *packet = parse_packet(conn2->rbuffer, &conn2->rlength, BUFFER_LENGTH);
                    if (!packet) {
                        // parse_packet 可能返回 NULL 表示不完整包或协议错误
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
			size_t avail = BUFFER_LENGTH - conn3->rlength;
			if (avail == 0) {
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


