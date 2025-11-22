// proactor_linked.c
// 完整文件：将每 connection 的 wbuffer 改为链表（每条消息一个 node），最小化改动。

#include "kvstore.h"
#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <errno.h>

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2
#define ENTRIES_LENGTH  1024
#define BUFFER_LENGTH   1024
#define MAX_CONN        65535

// 外部符号（保留为你项目里实现的函数/变量）
extern int kvs_protocol(char *msg, int length, char *response);
extern int kvs_ms_protocol(char *msg, int len, char *response);
extern char syncc[1024];
//extern int kvs_role;
extern const char *master_ip; // optional
extern int kvs_join_tokens(char *tokens[], int n, char *out);
extern int kvs_sync_msg(char *msg, int len);
extern void kvs_free(void *p);
extern char *parse_packet(char *msg, int *msg_len, int buffer_size);
extern int client_fds[]; // defined elsewhere
extern int client_count; // defined elsewhere
extern int master_port;
extern int slave_port;
//extern int MAX_CLIENTS; // keep as external if used
// If your build defines ENABLE_MS, it should be provided via -D or header
#ifndef ENABLE_MS
#define ENABLE_MS 0
#endif

// msg_node: 每条待发送消息
typedef struct msg_node {
    char *data;
    size_t len;
    size_t offset;
    struct msg_node *next;
} msg_node_t;

// connection_t: 保持其它字段，替换原来 wbuffer/wlength/woffset 为链表
typedef struct connection_s {
    int fd;
    char rbuffer[BUFFER_LENGTH];
    int rlength;
    int recv_pending;

    msg_node_t *send_head;
    msg_node_t *send_tail;
    int write_pending;
} connection_t;

static connection_t p_conn_list[MAX_CONN];

// io_uring 全局
struct io_uring ring;

// 简单客户端列表管理函数（若你原有位置不同，可忽略，保持行为）
extern int client_fds[]; // declared earlier
extern int client_count;
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

// set_event_* 函数与 conn_info
struct conn_info {
    int fd;
    int event;
};

int set_event_recv(struct io_uring *ringp, int sockfd, void *buf, size_t len, int flags) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ringp);
    if (!sqe) {
        fprintf(stderr, "[set_event_recv] failed to get SQE: submission queue full\n");
        return -1;
    }
    struct conn_info info = { .fd = sockfd, .event = EVENT_READ };
    io_uring_prep_recv(sqe, sockfd, buf, len, flags);
    memcpy(&sqe->user_data, &info, sizeof(struct conn_info));
    return 0;
}

int set_event_send(struct io_uring *ringp, int sockfd, void *buf, size_t len, int flags) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ringp);
    if (!sqe) {
        fprintf(stderr, "[set_event_send] failed to get SQE: submission queue full\n");
        return -1;
    }
    struct conn_info info = { .fd = sockfd, .event = EVENT_WRITE };
    io_uring_prep_send(sqe, sockfd, buf, len, flags);
    memcpy(&sqe->user_data, &info, sizeof(struct conn_info));
    return 0;
}

int set_event_accept(struct io_uring *ringp, int sockfd, struct sockaddr *addr, socklen_t *addrlen, int flags) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ringp);
    if (!sqe) {
        fprintf(stderr, "[set_event_accept] failed to get SQE: submission queue full\n");
        return -1;
    }
    struct conn_info info = { .fd = sockfd, .event = EVENT_ACCEPT };
    io_uring_prep_accept(sqe, sockfd, addr, addrlen, flags);
    memcpy(&sqe->user_data, &info, sizeof(struct conn_info));
    return 0;
}

// send queue helpers
static void enqueue_msg(connection_t *c, const char *msg, size_t len) {
    msg_node_t *node = malloc(sizeof(msg_node_t));
    if (!node) {
        fprintf(stderr, "[enqueue_msg] malloc node failed\n");
        return;
    }
    node->data = malloc(len);
    if (!node->data) {
        free(node);
        fprintf(stderr, "[enqueue_msg] malloc data failed\n");
        return;
    }
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

static void pop_msg(connection_t *c) {
    msg_node_t *n = c->send_head;
    if (!n) return;
    c->send_head = n->next;
    if (!c->send_head) c->send_tail = NULL;
    free(n->data);
    free(n);
}

// queue_send: 将一条消息放入链表尾部；若当前没有 pending，则触发一次 send
int queue_send(struct io_uring *ringp, int fd, char *data, size_t len) {
    if (fd < 0 || fd >= MAX_CONN || len == 0) return -1;
    connection_t *c = &p_conn_list[fd];
    // 如果 connection 未初始化，确保初始化字段
    if (c->fd != fd) {
        c->fd = fd;
        c->rlength = 0;
        c->recv_pending = 0;
        c->send_head = c->send_tail = NULL;
        c->write_pending = 0;
        memset(c->rbuffer, 0, BUFFER_LENGTH);
    }

    enqueue_msg(c, data, len);

    if (!c->write_pending) {
        msg_node_t *n = c->send_head;
        if (n) {
            c->write_pending = 1;
            if (set_event_send(ringp, fd, n->data + n->offset, n->len - n->offset, 0) < 0) {
                fprintf(stderr, "[queue_send] set_event_send failed for fd %d\n", fd);
                c->write_pending = 0;
                return -1;
            }
        }
    }
    return 0;
}

// proactor_broadcast: 保持你原先语义（把消息放到各连接链表），最后 submit ring
int proactor_broadcast(char *msg, size_t len) {
    if (client_count == 0) return -1;
    int any = 0;
    for (int i = 0; i < client_count; i++) {
        int fd = client_fds[i];
        int rc = queue_send(&ring, fd, msg, len);
        if (rc == 0) any++;
        else fprintf(stderr, "[proactor_broadcast] failed enqueue fd %d\n", fd);
    }
    if (any) {
        int sent = io_uring_submit(&ring);
        if (sent < 0) {
            fprintf(stderr, "[proactor_broadcast] io_uring_submit failed: %s\n", strerror(-sent));
        }
    }
    return any;
}

// helper: 打印可见字符（保留）
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

// minimal server init
int p_init_server(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }
    struct sockaddr_in serveraddr;
    memset(&serveraddr, 0, sizeof(struct sockaddr_in));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serveraddr.sin_port = htons(port);

    int on = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    if (-1 == bind(sockfd, (struct sockaddr*)&serveraddr, sizeof(struct sockaddr))) {
        perror("bind");
        close(sockfd);
        return -1;
    }
    if (listen(sockfd, 10) < 0) {
        perror("listen");
        close(sockfd);
        return -1;
    }
    return sockfd;
}

// handle write completion: ret = cqe->res
static void handle_write_event(int fd, int ret) {
    if (fd < 0 || fd >= MAX_CONN) return;
    connection_t *c = &p_conn_list[fd];
    if (!c) return;

    if (ret < 0) {
        // error
        if (ret == -EPIPE || ret == -ECONNRESET) {
            fprintf(stderr, "[proactor] write error on fd %d: %s, closing\n", fd, strerror(-ret));
            close(fd);
            c->write_pending = 0;
            // 清理链表
            while (c->send_head) pop_msg(c);
            remove_client_fd(fd);
            return;
        } else {
            fprintf(stderr, "[proactor] write error on fd %d: %s\n", fd, strerror(-ret));
            c->write_pending = 0;
            return;
        }
    }

    // 正常写入
    msg_node_t *n = c->send_head;
    if (!n) {
        c->write_pending = 0;
        return;
    }

    size_t written = (size_t)ret;
    if (written > n->len - n->offset) written = n->len - n->offset;
    n->offset += written;

    if (n->offset >= n->len) {
        // 完整发送了当前 node
        pop_msg(c);
        // 发送下一个（如果有）
        msg_node_t *next = c->send_head;
        if (next) {
            c->write_pending = 1;
            if (set_event_send(&ring, fd, next->data + next->offset, next->len - next->offset, 0) < 0) {
                fprintf(stderr, "[proactor] set_event_send failed for fd %d after pop\n", fd);
                // 发生错误时关闭连接以避免无限阻塞（可按需改）
                close(fd);
                // 清理
                while (c->send_head) pop_msg(c);
                remove_client_fd(fd);
                c->write_pending = 0;
            }
        } else {
            c->write_pending = 0;
        }
    } else {
        // 仍有未发送部分，继续发送
        if (set_event_send(&ring, fd, n->data + n->offset, n->len - n->offset, 0) < 0) {
            fprintf(stderr, "[proactor] set_event_send failed for fd %d to continue partial\n", fd);
            close(fd);
            while (c->send_head) pop_msg(c);
            remove_client_fd(fd);
            c->write_pending = 0;
        }
    }

    // 确保 recv 也已注册：若缓冲区可用则注册 recv（与你原逻辑一致）
    size_t avail = BUFFER_LENGTH - c->rlength;
    if (!c->recv_pending && avail > 0) {
        if (set_event_recv(&ring, fd, c->rbuffer + c->rlength, avail, 0) < 0) {
            fprintf(stderr, "[proactor] set_event_recv failed for fd %d after write\n", fd);
            close(fd);
            while (c->send_head) pop_msg(c);
            remove_client_fd(fd);
            c->recv_pending = 0;
        } else {
            c->recv_pending = 1;
        }
    }
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
    conn->write_pending = 0;
    conn->send_head = conn->send_tail = NULL;
    conn->recv_pending = 0;
    memset(conn->rbuffer, 0, BUFFER_LENGTH);
   // memset(conn->wbuffer, 0, BUFFER_LENGTH);
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
// global message handler pointer
typedef int (*msg_handler)(char *msg, int length, char *response);
static msg_handler kvs_handler = NULL;

// main proactor start (整合你的原逻辑并替换 write 处理)
int proactor_start(unsigned short port, msg_handler handler) {
    int sockfd = p_init_server(port);
    if (sockfd < 0) return -1;

    // 初始化监听 fd 在 p_conn_list（可选）
    if (sockfd >= 0 && sockfd < MAX_CONN) {
        connection_t *sc = &p_conn_list[sockfd];
        sc->fd = sockfd;
        sc->rlength = 0;
        sc->recv_pending = 0;
        sc->send_head = sc->send_tail = NULL;
        sc->write_pending = 0;
        memset(sc->rbuffer, 0, BUFFER_LENGTH);
    }

    printf("proactor listen port: %d (fd=%d)\n", port, sockfd);
    kvs_handler = handler;
    char *tokens[] = { "SYNCALL" };
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

    if (set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0) < 0) {
        fprintf(stderr, "set_event_accept failed\n");
        io_uring_queue_exit(&ring);
        close(sockfd);
        return -1;
    }
    io_uring_submit(&ring);
    printf("Accept event set successfully\n");

#if ENABLE_MS
    if (kvs_role == ROLE_SLAVE) {
        printf("[proactor] Trying to connect master %s:%d ...\n", master_ip, port);
        // proactor_connect 在原文件中实现，如果没有请自己实现（此处假设你已有）
        extern int proactor_connect(char *master_ip, unsigned short conn_port);
        int master_fd = proactor_connect((char*)master_ip, port);
        if (master_fd >= 0) {
            add_client_fd(master_fd);
            printf("[proactor] Connected to master (fd=%d)\n", master_fd);
            kvs_sync_msg(syncc, strlen(syncc));
        } else {
            fprintf(stderr, "[proactor] Failed to connect master %s:%d\n", master_ip, port);
        }
    }
#endif

    char response[BUFFER_LENGTH];

    // 主循环
    while (1) {
        io_uring_submit(&ring);

        struct io_uring_cqe *cqes[128];
        int nready = io_uring_peek_batch_cqe(&ring, cqes, 128); // 非阻塞 peek

        if (nready <= 0) {
            // 若没有事件，短暂休眠以避免忙等（可去掉）
            // usleep(1000);
            continue;
        }

        for (int i = 0; i < nready; i++) {
            struct io_uring_cqe *entry = cqes[i];
            struct conn_info info;
            memcpy(&info, &entry->user_data, sizeof(struct conn_info));

            if (info.event == EVENT_ACCEPT) {
                // 继续注册 accept
                if (set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0) < 0) {
                    fprintf(stderr, "[proactor_start] set_event_accept failed\n");
                }

                int connfd = entry->res;
                if (connfd < 0) {
                    // accept error
                    continue;
                }
                if (connfd >= MAX_CONN) {
                    close(connfd);
                    continue;
                }
                connection_t *conn = &p_conn_list[connfd];
                conn->fd = connfd;
                conn->rlength = 0;
                conn->recv_pending = 0;
                conn->send_head = conn->send_tail = NULL;
                conn->write_pending = 0;
                memset(conn->rbuffer, 0, BUFFER_LENGTH);

                // 注册 recv
                if (!conn->recv_pending) {
                    if (set_event_recv(&ring, connfd, conn->rbuffer + conn->rlength, BUFFER_LENGTH - conn->rlength, 0) < 0) {
                        fprintf(stderr, "[proactor_start] set_event_recv failed for fd %d\n", connfd);
                        close(connfd);
                        continue;
                    }
                    conn->recv_pending = 1;
                }

            } else if (info.event == EVENT_READ) {
                int r = entry->res;
                int fd = info.fd;
                if (r == 0) {
                    // peer closed
                    if (fd >= 0 && fd < MAX_CONN) p_conn_list[fd].recv_pending = 0;
                    close(fd);
                    continue;
                } else if (r < 0) {
                    // read error
                    fprintf(stderr, "[proactor] read error fd %d: %s\n", fd, strerror(-r));
                    if (fd >= 0 && fd < MAX_CONN) p_conn_list[fd].recv_pending = 0;
                    close(fd);
                    continue;
                } else if (r > 0) {
                    if (fd < 0 || fd >= MAX_CONN) {
                        close(fd);
                        continue;
                    }

                    connection_t *conn2 = &p_conn_list[fd];
                    conn2->recv_pending = 0;

                    size_t add = (size_t)r;
                    if (conn2->rlength + add > BUFFER_LENGTH) {
                        add = BUFFER_LENGTH - conn2->rlength;
                        printf("[proactor] Warning: buffer overflow for fd %d, truncating data\n", fd);
                    }
                    int before_len = conn2->rlength + add;
                    conn2->rlength += add;

#if ENABLE_MS
                    connection_t *conn_check = &p_conn_list[fd];
                    if (conn_check->rlength == (int)strlen(syncc) && memcmp(conn_check->rbuffer, syncc, conn_check->rlength) == 0) {
                        printf("get SYNC fd:%d\n", fd);
                        add_client_fd(fd);
                    }
#endif
                    char *packet = parse_packet(conn2->rbuffer, &conn2->rlength, BUFFER_LENGTH);
                    if (!packet) {
                        if (conn2->rlength == 0) {
                            fprintf(stderr, "[proactor] protocol error, closing fd %d, buf='%s'\n", fd, conn2->rbuffer);
                            close(fd);
                            conn2->recv_pending = 0;
                        } else {
                            // incomplete packet, 等待更多数据
                        }
                    } else {
                        int packet_len = before_len - conn2->rlength;
                        int outlen = 0;
                        if (kvs_handler) {
                            outlen = kvs_handler(packet, packet_len, response);
                        } else {
                            // 若没有 handler，按协议调用 kvs_protocol
                            outlen = kvs_protocol(packet, packet_len, response);
                        }
                        if (outlen > 0) {
                            // 通过 queue_send 发回响应
                            queue_send(&ring, fd, response, outlen);
                            io_uring_submit(&ring);
                        }
                        kvs_free(packet);
                    }

                    // 为该 fd 重新注册 recv（使用剩余空间）
                    size_t avail_after = BUFFER_LENGTH - conn2->rlength;
                    if (avail_after > 0) {
                        if (!conn2->recv_pending) {
                            if (set_event_recv(&ring, fd, conn2->rbuffer + conn2->rlength, avail_after, 0) < 0) {
                                fprintf(stderr, "[proactor] set_event_recv failed for fd %d after read\n", fd);
                                close(fd);
                                continue;
                            }
                            conn2->recv_pending = 1;
                        }
                    }
                }
            } else if (info.event == EVENT_WRITE) {
                int fd = info.fd;
                int r = entry->res;
                // 注意：entry->res 为写入字节数，负值为错误
                handle_write_event(fd, r);
            } else {
                // 未识别事件
            }
        }

        io_uring_cq_advance(&ring, nready);
    }

    // unreachable in current design; 如果需要退出需清理
    // io_uring_queue_exit(&ring);
    // close(sockfd);
    return 0;
}
