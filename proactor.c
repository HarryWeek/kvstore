
#include "kvstore.h"
#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdlib.h>

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2
#define ENTRIES_LENGTH  1024
#define BUFFER_LENGTH   1024
#define MAX_CONN        65535

extern int kvs_protocol(char *msg, int length, char *response);
extern int kvs_ms_protocol(char *msg, int len,char *response);
extern char syncc[1024];
extern int client_fds[MAX_CLIENTS];
extern int client_count;
extern int master_port;
extern int slave_port;

// =====================================================================================
//                               SEND QUEUE (LINKED LIST)
// =====================================================================================

typedef struct msg_node {
    char *data;
    size_t len;
    size_t offset;
    struct msg_node *next;
} msg_node;

// =====================================================================================
//                                CONNECTION STRUCT
// =====================================================================================

typedef struct connection_s {
    int fd;
    char rbuffer[BUFFER_LENGTH];
    int rlength;
    int recv_pending;

    msg_node *send_head;
    msg_node *send_tail;
    int write_pending;
} connection_t;

static connection_t p_conn_list[MAX_CONN];

// =====================================================================================
// Forward declarations
// =====================================================================================
int set_event_send(struct io_uring *ring, int sockfd, void *buf, size_t len, int flags);
int set_event_recv(struct io_uring *ring, int sockfd, void *buf, size_t len, int flags);

// =====================================================================================
//                               SEND QUEUE HELPERS
// =====================================================================================
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
static inline void enqueue_msg(connection_t *c, const char *msg, size_t len) {
    msg_node *node = malloc(sizeof(msg_node));
    node->data = malloc(len);
    memcpy(node->data, msg, len);
    node->len = len;
    node->offset = 0;
    node->next = NULL;

    if (!c->send_tail) {
        c->send_head = c->send_tail = node;
    } else {
        c->send_tail->next = node;
        c->send_tail = node;
    }
}

static inline void pop_msg(connection_t *c) {
    msg_node *n = c->send_head;
    if (!n) return;
    c->send_head = n->next;
    if (!c->send_head) c->send_tail = NULL;
    free(n->data);
    free(n);
}

// =====================================================================================
//                         queue_send: now enqueue + maybe trigger send
// =====================================================================================

int queue_send(struct io_uring *ring, int fd, char *data, size_t len) {
    if (fd < 0 || fd >= MAX_CONN || len == 0) return -1;
    connection_t *c = &p_conn_list[fd];

    enqueue_msg(c, data, len);

    // 若当前没有 write_pending，则立即触发一次 send
    if (!c->write_pending) {
        msg_node *n = c->send_head;
        if (n) {
            c->write_pending = 1;
            if (set_event_send(ring, fd, n->data + n->offset, n->len - n->offset, 0) < 0) {
                fprintf(stderr, "[queue_send] trigger send failed fd=%d", fd);
                c->write_pending = 0;
                return -1;
            }
        }
    }

    return 0;
}

// =====================================================================================
//                           proactor_broadcast: unchanged logic
// =====================================================================================


struct io_uring ring;

int proactor_broadcast(char *msg, size_t len) {
    int any = 0;
    for (int i = 0; i < client_count; i++) {
        int fd = client_fds[i];
        if (queue_send(&ring, fd, msg, len) == 0)
            any++;
    }
    if (any) io_uring_submit(&ring);
    return any;
}

// =====================================================================================
//                 EVENT_WRITE: send from linked list, pop on completion
// =====================================================================================

static inline void handle_write(struct io_uring *ring, int fd, int ret) {
    connection_t *c = &p_conn_list[fd];

    if (ret < 0) {
        fprintf(stderr, "[WRITE] error fd=%d", fd);
        close(fd);
        return;
    }

    msg_node *n = c->send_head;
    if (!n) {
        c->write_pending = 0;
        return;
    }

    n->offset += ret;

    if (n->offset >= n->len) {
        // 本条消息发完
        pop_msg(c);

        // 看链表里还有没有下一条
        msg_node *next = c->send_head;
        if (!next) {
            c->write_pending = 0;
            return;
        }

        // 发送下一条
        c->write_pending = 1;
        set_event_send(ring, fd, next->data + next->offset, next->len - next->offset, 0);
        return;
    }

    // 继续发送这一条剩余部分
    c->write_pending = 1;
    set_event_send(ring, fd, n->data + n->offset, n->len - n->offset, 0);
}

// =====================================================================================
//          以下全部是你的原始代码 + 最小修改（write 部分改为链表处理）
// =====================================================================================

// -- keep your original accept/read logic (only write part changed) --

struct conn_info { int fd; int event; };

int set_event_recv(struct io_uring *ring, int sockfd,
                   void *buf, size_t len, int flags) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) return -1;
    struct conn_info info = {.fd=sockfd, .event=EVENT_READ};
    io_uring_prep_recv(sqe, sockfd, buf, len, flags);
    memcpy(&sqe->user_data, &info, sizeof(info));
    return 0;
}

int set_event_send(struct io_uring *ring, int sockfd,
                   void *buf, size_t len, int flags) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) return -1;
    struct conn_info info = {.fd=sockfd, .event=EVENT_WRITE};
    io_uring_prep_send(sqe, sockfd, buf, len, flags);
    memcpy(&sqe->user_data, &info, sizeof(info));
    return 0;
}

int set_event_accept(struct io_uring *ring, int sockfd, struct sockaddr *addr,
                     socklen_t *addrlen, int flags) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) return -1;
    struct conn_info info = {.fd=sockfd, .event=EVENT_ACCEPT};
    io_uring_prep_accept(sqe, sockfd, addr, addrlen, flags);
    memcpy(&sqe->user_data, &info, sizeof(info));
    return 0;
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
    //conn->wlength = 0;
    conn->recv_pending = 0;
    memset(conn->rbuffer, 0, BUFFER_LENGTH);
    //memset(conn->wbuffer, 0, BUFFER_LENGTH);
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
// =====================================================================================
//                            MAIN PROACTOR LOOP (WRITE UPDATED)
// =====================================================================================


static msg_handler kvs_handler;

int proactor_start(unsigned short port, msg_handler handler) {
    //printf("proactor start\n");
	int sockfd = p_init_server(port);
    //初始化连接结构体数组
    connection_t *conn = &p_conn_list[sockfd];
    conn->fd=sockfd;
    conn->rlength=0;
    //conn->wlength=0;
    memset(conn->rbuffer,0,BUFFER_LENGTH);
    //memset(conn->wbuffer,0,BUFFER_LENGTH);

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
                if (connfd >= MAX_CONN) {
                    close(connfd);
                    continue;
                }
                // 初始化连接结构体数组
                connection_t *conn = &p_conn_list[connfd];
                conn->fd = connfd;
                conn->rlength = 0;
               // conn->wlength = 0;
                conn->recv_pending = 0;
                memset(conn->rbuffer, 0, BUFFER_LENGTH);
                //memset(conn->wbuffer, 0, BUFFER_LENGTH);
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
				}
			}  else if (result.event == EVENT_WRITE) {
  //
                handle_write(&ring, result.fd, entries->res);
				
			}
			
		}

		io_uring_cq_advance(&ring, nready);
	}

}

// =======================  END COMPLETE CODE =======================
