#include "kvstore.h"
#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2
#define ENTRIES_LENGTH  1024
#define BUFFER_LENGTH   1024
#define MAX_CONN 65535

extern int kvs_protocol(char *msg, int length, char *response);
extern int kvs_ms_protocol(char *msg, int len, char *response);

#if ENABLE_MS
extern int client_fds[MAX_CLIENTS];
extern int client_count;
extern int master_port;
extern int slave_port;
/* 注意：这里只声明一次全局 ring（删除重复声明） */
#endif

/* ------------------ retransmit config ------------------ */

#define MAX_OUTSTANDING 64
#define MAX_RETRIES 5
#define INITIAL_RTO_MS 200

typedef enum {
    MSG_TYPE_DATA = 0,
    MSG_TYPE_ACK = 1
} msg_type_t;

typedef struct outstanding_s {
    uint32_t msg_id;      // 应用层消息ID
    char *buf;            // 指向已打包发送数据（header + payload），由发送方 kvs_malloc
    int len;              // buf 长度
    int retries;          // 已重试次数
    long rto_ms;          // 当前 RTO (ms)
    struct timespec ts;   // 上次发送时间
    int pending;          // 是否仍未确认（1 = 未确认）
} outstanding_t;

typedef struct connection_s {
    int fd;
    char rbuffer[BUFFER_LENGTH];
    int rlength;
    char wbuffer[BUFFER_LENGTH];
    int wlength;
    int recv_pending;//0:没有挂起的接收 1:有挂起的接收

    /* retransmit */
    outstanding_t outstanding[MAX_OUTSTANDING]; // 存放未确认的消息
    int outstanding_count;
    uint32_t next_msg_id;
} connection_t;

/* connection array indexed by fd */
static connection_t p_conn_list[MAX_CONN];

/* 全局 io_uring，文件内只声明一次 */
struct io_uring ring;
char* trans_parse_packet(int fd, connection_t *conn,
                         char *msg, int *msg_len, int buffer_size);
/* pack/unpack fd+event into user_data pointer (uintptr_t) */
static inline void *pack_user_data(int fd, int event) {
    uintptr_t v = ((uintptr_t)fd << 2) | (uintptr_t)(event & 0x3);
    return (void *)v;
}
static inline void unpack_user_data(void *p, int *fd, int *event) {
    uintptr_t v = (uintptr_t)p;
    *event = (int)(v & 0x3);
    *fd = (int)(v >> 2);
}

/* time helpers */
static inline uint64_t now_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)(ts.tv_sec) * 1000 + (ts.tv_nsec) / 1000000;
}
static void record_timespec_now(struct timespec *ts) {
    clock_gettime(CLOCK_MONOTONIC, ts);
}
static int ms_since(struct timespec *ts) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    int diff = (int)((now.tv_sec - ts->tv_sec) * 1000 + (now.tv_nsec - ts->tv_nsec) / 1000000);
    return diff;
}

/* 打包 header + payload，返回 kvs_malloc 的 buffer，len_out 返回长度 */
static char *pack_msg(uint32_t msg_id, msg_type_t type, const char *payload, int payload_len, int *len_out) {
    int total = 4 + 1 + payload_len;
    char *buf = kvs_malloc(total);
    if (!buf) return NULL;
    uint32_t id_net = htonl(msg_id);
    memcpy(buf, &id_net, 4);
    buf[4] = (char)type;
    if (payload_len > 0 && payload) {
        memcpy(buf + 5, payload, payload_len);
    }
    *len_out = total;
    return buf;
}
static void unpack_header(const char *buf, uint32_t *msg_id, msg_type_t *type) {
    uint32_t id_net;
    memcpy(&id_net, buf, 4);
    *msg_id = ntohl(id_net);
    *type = (msg_type_t)buf[4];
}

/* 前置声明 */
int set_event_send(struct io_uring *ringp, int sockfd, void *buf, size_t len, int flags);
int set_event_recv(struct io_uring *ringp, int sockfd, void *buf, size_t len, int flags);
int set_event_accept(struct io_uring *ringp, int sockfd, struct sockaddr *addr, socklen_t *addrlen, int flags);

#if ENABLE_MS
static void add_client_fd(int fd) {
    for (int i = 0; i < client_count; i++) {
        if (client_fds[i] == fd) return;
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
#endif

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
void x_print_visible(char *buf, int len) {
    for (int i = 0; i < len; i++) {
        unsigned char c = buf[i];
        if (c >= 32 && c <= 126)
            printf("%c", c);
        else
            printf("\\x%02X", c);
    }
}
/* Connect to master (used by slave). Make socket non-blocking optional depending on requirement */
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

#if ENABLE_MS
    add_client_fd(sockfd);
#endif

    /* 初始化 connection 结构 */
    if (sockfd < 0 || sockfd >= MAX_CONN) {
        close(sockfd);
        return -1;
    }
    connection_t *conn = &p_conn_list[sockfd];
    conn->fd = sockfd;
    conn->rlength = 0;
    conn->wlength = 0;
    conn->recv_pending = 0;
    memset(conn->rbuffer, 0, BUFFER_LENGTH);
    memset(conn->wbuffer, 0, BUFFER_LENGTH);
    conn->outstanding_count = 0;
    conn->next_msg_id = 1;
    for (int i = 0; i < MAX_OUTSTANDING; i++) {
        conn->outstanding[i].pending = 0;
        conn->outstanding[i].buf = NULL;
    }

    if (set_event_recv(&ring, sockfd, conn->rbuffer, BUFFER_LENGTH, 0) < 0) {
        fprintf(stderr, "[proactor_connect] set_event_recv failed for fd %d\n", sockfd);
        close(sockfd);
        return -1;
    }
    conn->recv_pending = 1;

    /* 主循环会 submit，一些路径（如单独调用 proactor_connect）可以立即提交以尽快生效 */
    io_uring_submit(&ring);

    printf("[proactor] Connected to master %s:%d (fd=%d)\n", master_ip, conn_port, sockfd);
    return sockfd;
}

/* server init */
int p_init_server(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }

    int enable = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) < 0) {
        perror("setsockopt");
        /* non-fatal */
    }

    struct sockaddr_in serveraddr;
    memset(&serveraddr, 0, sizeof(struct sockaddr_in));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serveraddr.sin_port = htons(port);

    if (-1 == bind(sockfd, (struct sockaddr *)&serveraddr, sizeof(struct sockaddr))) {
        perror("bind");
        close(sockfd);
        return -1;
    }

    if (listen(sockfd, 128) < 0) {
        perror("listen");
        close(sockfd);
        return -1;
    }

    /* 设置非阻塞以配合 io_uring 非阻塞风格 */
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
    }

    return sockfd;
}

/* set recv SQE (不调用 submit，由主循环统一 submit) */
int set_event_recv(struct io_uring *ringp, int sockfd, void *buf, size_t len, int flags) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ringp);
    if (!sqe) {
        fprintf(stderr, "[set_event_recv] failed to get SQE: submission queue full\n");
        return -1;
    }
    io_uring_prep_recv(sqe, sockfd, buf, len, flags);
    io_uring_sqe_set_data(sqe, pack_user_data(sockfd, EVENT_READ));
    return 0;
}

/* set send SQE
   - 不再在内部调用 io_uring_submit（由主循环 batch submit）
   - 如果 fd 在 p_conn_list 范围内并且 len <= BUFFER_LENGTH，复制到 conn->wbuffer 避免栈地址问题
*/
int set_event_send(struct io_uring *ringp, int sockfd, void *buf, size_t len, int flags) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ringp);
    if (!sqe) {
        fprintf(stderr, "[set_event_send] failed to get SQE: submission queue full\n");
        return -1;
    }

    void *send_ptr = buf;
    if (sockfd >= 0 && sockfd < MAX_CONN && len <= BUFFER_LENGTH) {
        connection_t *c = &p_conn_list[sockfd];
        /* 保护：确保 conn 指向的缓冲区存在 */
        if (c) {
            memcpy(c->wbuffer, buf, len);
            c->wlength = (int)len;
            send_ptr = c->wbuffer;
        }
    } else {
        /* 如果太大或 fd 越界，必须分配临时缓冲并在写完成后释放。
           这里我们拒绝这个场景以保持最小修改：要求 len <= BUFFER_LENGTH 且 fd 合法。 */
        if (len > BUFFER_LENGTH || sockfd < 0 || sockfd >= MAX_CONN) {
            fprintf(stderr, "[set_event_send] invalid fd/len (fd=%d,len=%zu)\n", sockfd, len);
            return -1;
        }
    }

    io_uring_prep_send(sqe, sockfd, send_ptr, len, flags);
    io_uring_sqe_set_data(sqe, pack_user_data(sockfd, EVENT_WRITE));
    return 0;
}

/* set accept SQE */
int set_event_accept(struct io_uring *ringp, int sockfd, struct sockaddr *addr, socklen_t *addrlen, int flags) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ringp);
    if (!sqe) {
        fprintf(stderr, "[set_event_accept] failed to get SQE: submission queue full\n");
        return -1;
    }
    io_uring_prep_accept(sqe, sockfd, (struct sockaddr *)addr, addrlen, flags);
    io_uring_sqe_set_data(sqe, pack_user_data(sockfd, EVENT_ACCEPT));
    return 0;
}

/* -------------------- retransmit logic -------------------- */
int send_with_retransmit(connection_t *conn, const char *payload, int payload_len) {
    if (!conn) return -1;
    if (conn->outstanding_count >= MAX_OUTSTANDING) {
        fprintf(stderr, "[retransmit] outstanding full for fd %d\n", conn->fd);
        return -1;
    }
    uint32_t msg_id = conn->next_msg_id++;
    int packed_len = 0;
    char *packed = pack_msg(msg_id, MSG_TYPE_DATA, payload, payload_len, &packed_len);
    if (!packed) return -1;

    int idx = -1;
    for (int i = 0; i < MAX_OUTSTANDING; i++) {
        if (!conn->outstanding[i].pending) { idx = i; break; }
    }
    if (idx == -1) {
        kvs_free(packed);
        return -1;
    }

    outstanding_t *o = &conn->outstanding[idx];
    o->msg_id = msg_id;
    o->buf = packed;
    o->len = packed_len;
    o->retries = 0;
    o->rto_ms = INITIAL_RTO_MS;
    record_timespec_now(&o->ts);
    o->pending = 1;
    conn->outstanding_count++;

    /* 通过 set_event_send 将数据放入 SQE（注意主循环 later submit） */
    if (set_event_send(&ring, conn->fd, packed, packed_len, 0) < 0) {
        /* 发送失败，清理状态 */
        o->pending = 0;
        conn->outstanding_count--;
        kvs_free(packed);
        return -1;
    }

    return (int)msg_id;
}

static void check_retransmissions() {
    for (int fd = 0; fd < MAX_CONN; fd++) {
        connection_t *c = &p_conn_list[fd];
        if (c->fd <= 0) continue;
        for (int i = 0; i < MAX_OUTSTANDING; i++) {
            outstanding_t *o = &c->outstanding[i];
            if (!o->pending) continue;
            int elapsed = ms_since(&o->ts);
            if (elapsed >= o->rto_ms) {
                if (o->retries >= MAX_RETRIES) {
                    fprintf(stderr, "[retransmit] max retries reached for fd %d msg_id %u\n", c->fd, o->msg_id);
                    o->pending = 0;
                    if (o->buf) { kvs_free(o->buf); o->buf = NULL; }
                    c->outstanding_count--;
                } else {
                    if (set_event_send(&ring, c->fd, o->buf, o->len, 0) < 0) {
                        fprintf(stderr, "[retransmit] failed to resend for fd %d msg_id %u\n", c->fd, o->msg_id);
                    }
                    o->retries++;
                    o->rto_ms *= 2;
                    if (o->rto_ms > 60000) o->rto_ms = 60000;
                    record_timespec_now(&o->ts);
                    fprintf(stderr, "[retransmit] resent msg_id %u for fd %d, retry %d, new RTO %ld ms\n",
                            o->msg_id, c->fd, o->retries, o->rto_ms);
                }
            }
        }
    }
}

int proactor_broadcast(char *msg, size_t len) {
#if ENABLE_MS
    if (client_count == 0){
        printf("client_count is 0, no broadcast\n");
        return -1;
    } 
    for (int i = 0; i < client_count; i++) {
        int fd = client_fds[i];
        if (fd < 0 || fd >= MAX_CONN) continue;
        connection_t *conn = &p_conn_list[fd];
        if (conn->fd <= 0) continue;
        send_with_retransmit(conn, msg, (int)len);
    }
    return client_count;
#else
    (void)msg; (void)len;
    return -1;
#endif
}

extern char syncc[1024];
typedef int (*msg_handler)(char *msg, int length, char *response);
static msg_handler kvs_handler = NULL;

/* proactor_start: 初始化 io_uring、设置 accept SQE、进入事件循环 */
int proactor_start(unsigned short port, msg_handler handler) {
    int sockfd = p_init_server(port);
    if (sockfd < 0) return -1;

    /* 初始化监听 socket 的 connection_t（方便复用数组索引） */
    if (sockfd >= 0 && sockfd < MAX_CONN) {
        connection_t *conn = &p_conn_list[sockfd];
        conn->fd = sockfd;
        conn->rlength = 0;
        conn->wlength = 0;
        conn->recv_pending = 0;
        memset(conn->rbuffer, 0, BUFFER_LENGTH);
        memset(conn->wbuffer, 0, BUFFER_LENGTH);
        conn->outstanding_count = 0;
        conn->next_msg_id = 1;
        for (int i = 0; i < MAX_OUTSTANDING; i++) {
            conn->outstanding[i].pending = 0;
            conn->outstanding[i].buf = NULL;
        }
    }

    printf("proactor listen port: %d\n", port);
    kvs_handler = handler;
    char *tokens[] = {"SYNCALL"};
    kvs_join_tokens(tokens, 1, syncc);

    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    int ret = io_uring_queue_init_params(ENTRIES_LENGTH, &ring, &params);
    if (ret < 0) {
        fprintf(stderr, "io_uring_queue_init_params failed: %s\n", strerror(-ret));
        close(sockfd);
        return -1;
    }
    printf("io_uring initialized successfully\n");

    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);

    /* 设置 accept SQE */
    if (set_event_accept(&ring, sockfd, (struct sockaddr *)&clientaddr, &len, 0) < 0) {
        fprintf(stderr, "set_event_accept failed\n");
        io_uring_queue_exit(&ring);
        close(sockfd);
        return -1;
    }
    /* 提交 accept SQE（主循环也会在每次循环顶部提交，提前提交一次可尽快开始监听） */
    io_uring_submit(&ring);
    printf("Accept event set successfully\n");

    char response[BUFFER_LENGTH] = {0};

#if ENABLE_MS
    extern char *master_ip;
    if (kvs_role == ROLE_SLAVE) {
        printf("[proactor] Trying to connect master %s:%d ...\n", master_ip, port);
        int master_fd = proactor_connect(master_ip, port);
        if (master_fd >= 0) {
            add_client_fd(master_fd);
            printf("[proactor] Connected to master (fd=%d)\n", master_fd);
            send_with_retransmit(&p_conn_list[master_fd], syncc, (int)strlen(syncc));
        } else {
            fprintf(stderr, "[proactor] Failed to connect master %s:%d\n", master_ip, port);
        }
    }
#endif

    /* 主事件循环 */
    while (1) {
        /* 提交所有已准备的 SQE（set_event_* 不会再内部 submit） */
        io_uring_submit(&ring);

        struct io_uring_cqe *cqes[128];
        int nready = io_uring_peek_batch_cqe(&ring, cqes, 128);
        if (nready == 0) {
            struct io_uring_cqe *single = NULL;
            io_uring_wait_cqe(&ring, &single);
            if (single != NULL) {
                cqes[0] = single;
                nready = 1;
            }
        }

        for (int i = 0; i < nready; i++) {
            //printf("nready=%d\n",nready);
            struct io_uring_cqe *entry = cqes[i];
            int ev_fd = -1, ev_event = -1;
            void *ud = io_uring_cqe_get_data(entry);
            if (ud) unpack_user_data(ud, &ev_fd, &ev_event);
            printf("[proactor] Event on fd %d, type %d, res %d\n", ev_fd, ev_event, entry->res);
            if (ev_event == EVENT_ACCEPT) {
                /* 重新注册 accept */
                if (set_event_accept(&ring, sockfd, (struct sockaddr *)&clientaddr, &len, 0) < 0) {
                    fprintf(stderr, "[proactor_start] set_event_accept failed\n");
                }

                int connfd = entry->res;
                if (connfd < 0) {
                    /* accept error */
                    continue;
                }
                if (connfd >= MAX_CONN) {
                    close(connfd);
                    continue;
                }

                /* 设置非阻塞 */
                int flags = fcntl(connfd, F_GETFL, 0);
                if (flags >= 0) fcntl(connfd, F_SETFL, flags | O_NONBLOCK);

                connection_t *conn = &p_conn_list[connfd];
                conn->fd = connfd;
                conn->rlength = 0;
                conn->wlength = 0;
                conn->recv_pending = 0;
                memset(conn->rbuffer, 0, BUFFER_LENGTH);
                memset(conn->wbuffer, 0, BUFFER_LENGTH);
                conn->outstanding_count = 0;
                conn->next_msg_id = 1;
                for (int j = 0; j < MAX_OUTSTANDING; j++) {
                    conn->outstanding[j].pending = 0;
                    conn->outstanding[j].buf = NULL;
                }

                /* 为新连接注册 recv */
                if (!conn->recv_pending) {
                    if (set_event_recv(&ring, connfd, conn->rbuffer + conn->rlength, BUFFER_LENGTH - conn->rlength, 0) < 0) {
                        fprintf(stderr, "[proactor_start] set_event_recv failed for fd %d\n", connfd);
                        close(connfd);
                        continue;
                    }
                    conn->recv_pending = 1;
                }

            } else if (ev_event == EVENT_READ) {
                int res = entry->res;
                int result_fd = ev_fd;
                if (res == 0) {
                    /* peer closed */
                    if (result_fd >= 0 && result_fd < MAX_CONN) {
                        p_conn_list[result_fd].recv_pending = 0;
                    }
#if ENABLE_MS
                    remove_client_fd(result_fd);
#endif
                    if (result_fd >= 0 && result_fd < MAX_CONN) {
                        connection_t *c = &p_conn_list[result_fd];
                        for (int j = 0; j < MAX_OUTSTANDING; j++) {
                            if (c->outstanding[j].pending) {
                                if (c->outstanding[j].buf) kvs_free(c->outstanding[j].buf);
                                c->outstanding[j].buf = NULL;
                                c->outstanding[j].pending = 0;
                            }
                        }
                        c->outstanding_count = 0;
                    }
                    close(result_fd);
                    if (result_fd >= 0 && result_fd < MAX_CONN) p_conn_list[result_fd].fd = 0;
                    /* nothing more to do for this cqe */
                } else if (res > 0) {
                    if (result_fd < 0 || result_fd >= MAX_CONN) {
                        close(result_fd);
                        continue;
                    }
                    connection_t *conn2 = &p_conn_list[result_fd];
                    conn2->recv_pending = 0;

                    /* 安全打印：避免 %s 直接打印未结束字符串 */
                    printf("get msg (%d bytes)\n", res);
                    x_print_visible(conn2->rbuffer, res);
                    printf("\n");
                    int MS = 0;
#if ENABLE_MS
                    for (int k = 0; k < client_count; k++) {
                        if (client_fds[k] == result_fd) { MS = 1; break; }
                    }
#endif
                    /* append */
                    size_t add = (size_t)res;
                    if (conn2->rlength + add > BUFFER_LENGTH) {
                        add = BUFFER_LENGTH - conn2->rlength;
                        printf("[proactor] Warning: buffer overflow for fd %d, truncating data\n", result_fd);
                    }
                    int before_len = conn2->rlength + (int)add;
                    conn2->rlength += (int)add;

                    if (!MS) {
                        /* 普通消息处理 */
                        char *packet = parse_packet(conn2->rbuffer, &conn2->rlength, BUFFER_LENGTH);
                        if (!packet) {
                            if (conn2->rlength == 0) {
                                fprintf(stderr, "[proactor] protocol error, closing fd %d\n", result_fd);
                                close(result_fd);
                                conn2->recv_pending = 0;
                            } else {
                                /* 等待更多数据 */
                            }
                        } else {
                                    // printf("received packet: ");
                                    // print_visible(packet);
                                    // printf("\n");
                                    // print_visible(syncc);
                                    // printf("\n");
                                    if(strcmp(packet,syncc)==0){
                                        add_client_fd(result_fd);
                                    }
                            int packet_len = before_len - conn2->rlength;
                            int outlen = 0;
                            if (kvs_handler) outlen = kvs_handler(packet, packet_len, response);
                            if (outlen > 0) {
                                if (set_event_send(&ring, result_fd, response, outlen, 0) < 0) {
                                    fprintf(stderr, "[proactor] set_event_send failed for fd %d\n", result_fd);
                                    close(result_fd);
                                }
                            }
                            kvs_free(packet);
                        }
                    } else {
                        /* 主从消息处理 */
                        char *packet = trans_parse_packet(result_fd, conn2, conn2->rbuffer, &conn2->rlength, BUFFER_LENGTH);

                        if (!packet) {
                            if (conn2->rlength == 0) {
#if ENABLE_MS
                                remove_client_fd(result_fd);
#endif
                                close(result_fd);
                                conn2->recv_pending = 0;
                            } else {
                                /* partial, wait for more */
                            }
                        } else {

#if 1
                            printf("received ms packet: ");
                            print_visible(packet+5);
                            printf("\n");
                            int packet_len = before_len - conn2->rlength;
                            if (packet_len < 5) {
                                kvs_free(packet);
                            } else {
                                uint32_t msg_id;
                                msg_type_t type;
                                unpack_header(packet, &msg_id, &type);
                                if (type == MSG_TYPE_ACK) {
                                    connection_t *c = &p_conn_list[result_fd];
                                    for (int k = 0; k < MAX_OUTSTANDING; k++) {
                                        outstanding_t *o = &c->outstanding[k];
                                        if (o->pending && o->msg_id == msg_id) {
                                            if (o->buf) kvs_free(o->buf);
                                            o->buf = NULL;
                                            o->pending = 0;
                                            c->outstanding_count--;
                                            o->len = 0;
                                            break;
                                        }
                                    }
                                } else if (type == MSG_TYPE_DATA) {
                                    int payload_len = packet_len - 5;
                                    int resp_len = 0;
                                    print_visible(packet + 5);

                                    if (kvs_handler) resp_len = kvs_handler(packet + 5, payload_len, response);
                                    char ack_buf[5];
                                    uint32_t id_net = htonl(msg_id);
                                    memcpy(ack_buf, &id_net, 4);
                                    ack_buf[4] = (char)MSG_TYPE_ACK;
                                    if (set_event_send(&ring, result_fd, ack_buf, 5, 0) < 0) {
                                        fprintf(stderr, "[proactor] set_event_send (ack) failed for fd %d\n", result_fd);
                                    }
                                    if (resp_len > 0) {
                                        if (set_event_send(&ring, result_fd, response, resp_len, 0) < 0) {
                                            fprintf(stderr, "[proactor] set_event_send (resp) failed for fd %d\n", result_fd);
                                        }
                                    }
                                }
                                kvs_free(packet);
                            }
#endif
                        }
                    }

                    /* 重新注册 recv（使用当前剩余空间） */
                    size_t avail_after = BUFFER_LENGTH - conn2->rlength;
                    if (avail_after > 0 && !conn2->recv_pending) {
                        if (set_event_recv(&ring, result_fd, conn2->rbuffer + conn2->rlength, avail_after, 0) < 0) {
                            fprintf(stderr, "[proactor] set_event_recv failed for fd %d after read\n", result_fd);
                            close(result_fd);
                            continue;
                        }
                        conn2->recv_pending = 1;
                    }
                } else {
                    /* res < 0: error on recv */
                    int err = entry->res;
                    fprintf(stderr, "[proactor] read error on fd %d: %s\n", result_fd, strerror(err));
                    if (result_fd >= 0 && result_fd < MAX_CONN) {
                        connection_t *c = &p_conn_list[result_fd];
                        for (int j = 0; j < MAX_OUTSTANDING; j++) {
                            if (c->outstanding[j].pending) {
                                if (c->outstanding[j].buf) kvs_free(c->outstanding[j].buf);
                                c->outstanding[j].buf = NULL;
                                c->outstanding[j].pending = 0;
                            }
                        }
                        c->outstanding_count = 0;
                    }
                    close(result_fd);
                }
            } else if (ev_event == EVENT_WRITE) {
                //printf("[proactor] write completed on fd %d, res %d\n", ev_fd, entry->res);
                int written = entry->res;
                int result_fd = ev_fd;
                if (result_fd < 0 || result_fd >= MAX_CONN) { close(result_fd); continue; }
                connection_t *conn3 = &p_conn_list[result_fd];
                size_t avail = BUFFER_LENGTH - conn3->rlength;
                printf("avail: %zu\n", avail);
                
                if (avail == 0) {
                    printf("[proactor] Warning: write completed but no space to recv on fd %d\n", result_fd);
                    conn3->recv_pending = 0;
                    avail = BUFFER_LENGTH;
                }
                printf("recv_pending: %d\n", conn3->recv_pending);
                if (!conn3->recv_pending) {
                    if (set_event_recv(&ring, result_fd, conn3->rbuffer + conn3->rlength, avail, 0) < 0) {
                        fprintf(stderr, "[proactor] set_event_recv failed for fd %d after write\n", result_fd);
                        close(result_fd);
                        continue;
                    }else{
                        printf("[proactor] set_event_recv success for fd %d after write\n", result_fd);
                    }
                    conn3->recv_pending = 1;
                }
            } else {
                /* 未知 event：忽略或 log */
                // fprintf(stderr, "unknown event %d\n", ev_event);
            }
        } /* end for each cqe */

        /* retransmit 和批量提交 (caveat: set_event_* 已在循环顶部提交) */
        check_retransmissions();
        io_uring_cq_advance(&ring, nready);
    } /* end while */

    /* unreachable, 但保持接口 */
    return 0;
}


char* trans_parse_packet(int fd, connection_t *conn,
                         char *msg, int *msg_len, int buffer_size)
{
    if (*msg_len <= 0) return NULL;

    int offset = 0;
    int total_used = 0;

    while (1) {
        if (*msg_len - offset < 8) break;   // 至少 msg_id(4) + type(1) + "#x\r\n"(3)

        /* '#' 必须在 offset+5 */
        if (msg[offset + 5] != '#') {
            char *p = memchr(msg + offset + 5, '#', *msg_len - (offset + 5));
            if (p) {
                int skip = p - (msg + offset) - 5;
                memmove(msg, msg + offset + 5 + skip,
                        *msg_len - (offset + 5 + skip));
                *msg_len -= (offset + 5 + skip);
                offset = 0;
                continue;
            } else {
                if (*msg_len > buffer_size/2) {
                    *msg_len = 0;
                    return NULL;
                } else break;
            }
        }

        int header_off = offset + 5;
        int remaining = *msg_len - header_off;
        char *rn = memmem(msg + header_off, remaining, "\r\n", 2);
        if (!rn) break;   // header 不完整

        int header_len = (rn - (msg + header_off)) + 2;

        /* 解析 body 长度 */
        int body_len = 0;
        char *pnum = msg + header_off + 1;
        char *pnum_end = rn;
        for (char *t = pnum; t < pnum_end; t++) {
            if (!isdigit((unsigned char)*t)) { *msg_len = 0; return NULL; }
            body_len = body_len * 10 + (*t - '0');
        }
        if (body_len > buffer_size) { *msg_len = 0; return NULL; }

        int packet_len = 5 + header_len + body_len;

        if (*msg_len - offset < packet_len) break;  // 半包

        offset += packet_len;
        total_used = offset;
    }

    if (total_used == 0) return NULL;

    /* 分配完整消息块 */
    char *full_packet = kvs_malloc(total_used);
    if (!full_packet) { *msg_len = 0; return NULL; }
    memcpy(full_packet, msg, total_used);

    /* 移动剩余消息 */
    int remain = *msg_len - total_used;
    if (remain > 0) memmove(msg, msg + total_used, remain);
    *msg_len = remain;

    /* --- 开始处理消息头部（msg_id + type） --- */

    uint32_t msg_id;
    msg_type_t type;
    memcpy(&msg_id, full_packet, 4);
    msg_id = ntohl(msg_id);
    type = (msg_type_t)full_packet[4];

    if (type == MSG_TYPE_ACK) {
        /* 处理 pending */
        for (int i = 0; i < MAX_OUTSTANDING; i++) {
            outstanding_t *o = &conn->outstanding[i];
            if (o->pending && o->msg_id == msg_id) {
                if (o->buf) kvs_free(o->buf);
                o->buf = NULL;
                o->pending = 0;
                conn->outstanding_count--;
                break;
            }
        }
        kvs_free(full_packet);
        return NULL;    // ACK 不需要返回给上层
    }

    /* ---------------- DATA 消息 ------------------ */

    /* 给对端发 ACK */
    char ack_buf[5];
    uint32_t id_net = htonl(msg_id);
    memcpy(ack_buf, &id_net, 4);
    ack_buf[4] = MSG_TYPE_ACK;

    set_event_send(&ring, fd, ack_buf, 5, 0);

    /* 返回 payload（剔除 msg_id + type） */
    int payload_len = total_used - 5;
    char *payload = kvs_malloc(payload_len + 1);
    if (!payload) {
        kvs_free(full_packet);
        return NULL;
    }

    memcpy(payload, full_packet + 5, payload_len);
    payload[payload_len] = '\0';

    kvs_free(full_packet);
    return payload;
}
