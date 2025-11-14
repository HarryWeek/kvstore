// reactor_server.c
// 改进版 reactor，使用动态发送缓冲区避免广播大量消息时丢失或覆盖。
// 直接替换或参考修改你的实现。

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
#include"kvstore.h"
#define CONNECTION_SIZE      65536
#define MAX_PORTS            1
//#define MAX_CLIENTS          10240
#define BACKLOG              128
#define RECV_BUFFER_SIZE     4096  // 用于 recv 的固定栈缓冲
#define INITIAL_WBUF_CAP     4096  // 每个连接发送缓冲初始容量
#define MAX_WBUF_LIMIT       (64 * 1024 * 1024) // 每连接发送队列上限（64MB）可按需调整

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

typedef int (*msg_handler)(char *msg, int length, char *response);

// -- 简化版连接结构（根据你原先使用的字段整理，保持兼容性） --
struct recv_action {
    int (*recv_callback)(int fd);
};

struct conn {
    int fd;
    // recv
    char rbuffer[RECV_BUFFER_SIZE];
    int rlength;
    struct recv_action r_action;

    // dynamic write buffer
    char *wbuf;       // 动态分配的发送缓冲区起始地址
    size_t wlen;      // 当前有效数据长度
    size_t wcap;      // 当前缓冲区容量

    // callback
    int (*send_callback)(int fd);
};

static int epfd = -1;
static struct timeval begin;

static struct conn conn_list[CONNECTION_SIZE];
extern int client_fds[MAX_CLIENTS];
extern int client_count;

static msg_handler kvs_handler = NULL;
char *syncc = "*1\r\n$7\r\nSYNCALL\r\n";
//int kvs_role = 0; // placeholder for ROLE_SLAVE check;  set appropriately where你集成


// helper: set non-blocking
static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) flags = 0;
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("[set_nonblocking] fcntl");
        return -1;
    }
    return 0;
}

// safe realloc wrapper
static int ensure_wbuf_capacity(struct conn *c, size_t need) {
    if (!c) return -1;
    size_t want = c->wlen + need;
    if (want <= c->wcap) return 0;
    size_t newcap = c->wcap ? c->wcap : INITIAL_WBUF_CAP;
    while (newcap < want) {
        newcap *= 2;
        if (newcap > MAX_WBUF_LIMIT) {
            // 达到上限，拒绝增长
            return -1;
        }
    }
    char *p = realloc(c->wbuf, newcap);
    if (!p) {
        return -1;
    }
    c->wbuf = p;
    c->wcap = newcap;
    return 0;
}

// epoll add/mod wrapper
static int set_event(int fd, uint32_t events, int add_not_mod) {
    if (epfd < 0) return -1;
    struct epoll_event ev;
    ev.events = events;
    ev.data.fd = fd;
    int op = add_not_mod ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
    if (epoll_ctl(epfd, op, fd, &ev) == -1) {
        if (add_not_mod && errno == EEXIST) {
            // already exists -> try modify
            if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) == -1) {
                perror("[set_event] EPOLL_CTL_MOD fallback failed");
                return -1;
            }
            return 0;
        }
        perror("[set_event] epoll_ctl");
        return -1;
    }
    return 0;
}

static int event_register(int fd, uint32_t events) {
    if (fd < 0) return -1;
    if (fd >= CONNECTION_SIZE) return -1;

    conn_list[fd].fd = fd;
    conn_list[fd].r_action.recv_callback = NULL; // caller should set appropriate callback
    conn_list[fd].send_callback = NULL;

    conn_list[fd].rlength = 0;

    // init dynamic send buffer
    conn_list[fd].wbuf = NULL;
    conn_list[fd].wlen = 0;
    conn_list[fd].wcap = 0;

    // set non-blocking
    set_nonblocking(fd);

    // add to epoll
    return set_event(fd, events, 1);
}

// add to client list if not exist
static void add_client_fd(int fd) {
    for (int i = 0; i < client_count; i++) if (client_fds[i] == fd) return;
    if (client_count < MAX_CLIENTS) client_fds[client_count++] = fd;
}

// remove client fd from list
static void remove_client_fd(int fd) {
    for (int i = 0; i < client_count; i++) {
        if (client_fds[i] == fd) {
            client_fds[i] = client_fds[--client_count];
            return;
        }
    }
}

// close and cleanup connection
static void close_connection(int fd) {
    if (fd < 0 || fd >= CONNECTION_SIZE) return;
    // free dynamic buffer
    if (conn_list[fd].wbuf) {
        free(conn_list[fd].wbuf);
        conn_list[fd].wbuf = NULL;
    }
    conn_list[fd].wlen = 0;
    conn_list[fd].wcap = 0;

    // remove from epoll and close
    if (epfd >= 0) epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
    remove_client_fd(fd);
    conn_list[fd].fd = 0;
}

// forward declarations
int accept_cb(int fd);
int recv_cb(int fd);
int send_cb(int fd);

int reactor_connect(char *master_ip, unsigned short conn_port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("[reactor_connect] socket");
        return -1;
    }

    struct sockaddr_in serveraddr;
    memset(&serveraddr, 0, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_port = htons(conn_port);

    if (inet_pton(AF_INET, master_ip, &serveraddr.sin_addr) <= 0) {
        perror("[reactor_connect] inet_pton");
        close(sockfd);
        return -1;
    }

    if (connect(sockfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) < 0) {
        if (errno != EINPROGRESS) {
            perror("[reactor_connect] connect");
            close(sockfd);
            return -1;
        }
        // 非阻塞连接进行中也可接受
    }

    set_nonblocking(sockfd);

    printf("[reactor_connect] Connected to master %s:%d (fd=%d)\n", master_ip, conn_port, sockfd);
    return sockfd;
}

int kvs_request(struct conn *c) {
    if (!c || !kvs_handler) return 0;
    // 确保 rbuffer 以 \0 结束（handler 可能期望字符串）
    if (c->rlength >= RECV_BUFFER_SIZE) c->rbuffer[RECV_BUFFER_SIZE-1] = '\0';
    else c->rbuffer[c->rlength] = '\0';

    // 将响应写入临时缓冲，再追加到动态 wbuf
    char resp[RECV_BUFFER_SIZE];
    int wlen = kvs_handler(c->rbuffer, c->rlength, resp);
    if (wlen > 0) {
        // append resp[0..wlen-1] 到 c->wbuf
        if (ensure_wbuf_capacity(c, (size_t)wlen) != 0) {
            fprintf(stderr, "[kvs_request] cannot grow buffer for fd=%d\n", c->fd);
            return -1;
        }
        memcpy(c->wbuf + c->wlen, resp, wlen);
        c->wlen += wlen;
    }
    return wlen;
}

// recv callback: read data and prepare response
int recv_cb(int fd) {
    if (fd < 0 || fd >= CONNECTION_SIZE) return -1;
    struct conn *c = &conn_list[fd];
    if (!c) return -1;

    ssize_t n = recv(fd, c->rbuffer, sizeof(c->rbuffer) - 1, 0);
    if (n == 0) {
        // peer closed
        close_connection(fd);
        return 0;
    } else if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        perror("[recv_cb] recv");
        close_connection(fd);
        return -1;
    }

    c->rlength = (int)n;
    // 根据不同模式处理请求
    // 这里简化：如果是 kvstore 模式则调用 kvs_request，其他模式留空/或可扩展
    if (kvs_handler) {
        int ret = kvs_request(c);
        (void)ret;
    }

    // 如果写缓冲中有数据，确保 EPOLLOUT 被监听
    struct epoll_event ev;
    ev.data.fd = fd;
    ev.events = EPOLLIN | (c->wlen > 0 ? EPOLLOUT : 0);
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) == -1) {
        perror("[recv_cb] epoll_ctl MOD");
        // but continue
    }
    return (int)n;
}

// send routine: 尽可能把 wbuf 数据发送出去，若发送还剩余则开启 EPOLLOUT
int send_cb(int fd) {
    if (fd < 0 || fd >= CONNECTION_SIZE) return -1;
    struct conn *c = &conn_list[fd];
    if (!c) return -1;

    if (c->wlen == 0) {
        // nothing to send, ensure EPOLLOUT not set
        struct epoll_event ev;
        ev.data.fd = fd;
        ev.events = EPOLLIN;
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
        return 0;
    }

    ssize_t total_sent = 0;
    while (c->wlen > 0) {
        ssize_t sent = send(fd, c->wbuf, c->wlen, 0);
        if (sent > 0) {
            // remove sent bytes from head
            if ((size_t)sent < c->wlen) {
                memmove(c->wbuf, c->wbuf + sent, c->wlen - sent);
            }
            c->wlen -= (size_t)sent;
            total_sent += sent;
            // optionally shrink buffer if too large and almost empty (省略)
        } else if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 内核缓冲区满，等待下一次 EPOLLOUT
                break;
            } else if (errno == EINTR) {
                continue;
            } else {
                // unrecoverable error
                perror("[send_cb] send");
                close_connection(fd);
                return -1;
            }
        } else {
            // sent == 0: 表示对端关闭?
            close_connection(fd);
            return -1;
        }
    }

    // 更新 epoll 监听：如果还有未发数据，监听 EPOLLOUT；否则只监听 EPOLLIN
    struct epoll_event ev;
    ev.data.fd = fd;
    ev.events = EPOLLIN | (c->wlen > 0 ? EPOLLOUT : 0);
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) == -1) {
        perror("[send_cb] epoll_ctl MOD");
    }

    return (int)total_sent;
}

// broadcast：把 msg 追加到每个 client 的动态发送队列，并尽可能立即发送
int reactor_broadcast(const char *msg, size_t len) {
    if (!msg || len == 0) return -1;
    if (client_count == 0) return 0;

    int sent_count = 0;

    for (int i = 0; i < client_count; i++) {
        int fd = client_fds[i];
        if (fd <= 0 || fd >= CONNECTION_SIZE) continue;
        struct conn *c = &conn_list[fd];
        if (c->fd <= 0) continue;

        // 限制每连接排队上限，避免内存爆掉
        if (c->wlen + len > MAX_WBUF_LIMIT) {
            fprintf(stderr, "[reactor_broadcast] fd=%d exceed wbuf limit, skipping\n", fd);
            continue;
        }

        // 扩容并追加
        if (ensure_wbuf_capacity(c, len) != 0) {
            fprintf(stderr, "[reactor_broadcast] fd=%d cannot grow wbuf, skipping\n", fd);
            continue;
        }
        memcpy(c->wbuf + c->wlen, msg, len);
        c->wlen += len;

        // 尝试立刻发送尽可能多的数据（非阻塞）
        send_cb(fd);

        // 修改 epoll：确保 EPOLLOUT 在还有未发数据时被监听
        struct epoll_event ev;
        ev.data.fd = fd;
        ev.events = EPOLLIN | (c->wlen > 0 ? EPOLLOUT : 0);
        if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) == -1) {
            // 如果还没注册（极少情况）则尝试 ADD
            if (errno == ENOENT) {
                epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
            } else {
                perror("[reactor_broadcast] epoll_ctl MOD");
            }
        }

        sent_count++;
    }

    return sent_count;
}

// accept callback
int accept_cb(int fd) {
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);
    int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
    if (clientfd < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            perror("[accept_cb] accept");
        }
        return -1;
    }

    // 初始化 conn
    if (clientfd >= CONNECTION_SIZE) {
        fprintf(stderr, "[accept_cb] fd %d >= CONNECTION_SIZE %d, closing\n", clientfd, CONNECTION_SIZE);
        close(clientfd);
        return -1;
    }

    conn_list[clientfd].fd = clientfd;
    conn_list[clientfd].r_action.recv_callback = recv_cb;
    conn_list[clientfd].send_callback = send_cb;
    conn_list[clientfd].rlength = 0;
    conn_list[clientfd].wlen = 0;
    conn_list[clientfd].wcap = 0;
    conn_list[clientfd].wbuf = NULL;

    set_nonblocking(clientfd);

    // add to epoll: listen for read initially
    struct epoll_event ev;
    ev.data.fd = clientfd;
    ev.events = EPOLLIN;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, clientfd, &ev) == -1) {
        perror("[accept_cb] epoll_ctl ADD client");
        close(clientfd);
        return -1;
    }

    // add to client list
    add_client_fd(clientfd);

    return 0;
}

// create server socket, bind, listen
int r_init_server(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("[r_init_server] socket");
        return -1;
    }

    int on = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);

    if (bind(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) == -1) {
        perror("[r_init_server] bind");
        close(sockfd);
        return -1;
    }

    if (listen(sockfd, BACKLOG) == -1) {
        perror("[r_init_server] listen");
        close(sockfd);
        return -1;
    }

    // set non-blocking to accept continuously
    set_nonblocking(sockfd);

    printf("reactor listen:%d (fd=%d)\n", port, sockfd);
    return sockfd;
}

// main start
int reactor_start(unsigned short port, msg_handler handler) {
    kvs_handler = handler;

    epfd = epoll_create1(0);
    if (epfd == -1) {
        perror("[reactor_start] epoll_create1");
        return -1;
    }

    for (int i = 0; i < MAX_PORTS; i++) {
        int listenfd = r_init_server(port + i);
        if (listenfd < 0) continue;

        // init conn entry for listen fd
        conn_list[listenfd].fd = listenfd;
        conn_list[listenfd].r_action.recv_callback = accept_cb;
        conn_list[listenfd].send_callback = NULL;

        // add listen fd to epoll
        struct epoll_event ev;
        ev.data.fd = listenfd;
        ev.events = EPOLLIN;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev) == -1) {
            perror("[reactor_start] epoll_ctl ADD listenfd");
            close(listenfd);
            continue;
        }
    }

    // 如果作为 slave，尝试连接 master 并 broadcast sync（保留你的逻辑）
    // extern char *master_ip; // 如果有可以解注释并使用
#if ENABLE_MS
    extern char *master_ip;
    if (kvs_role == ROLE_SLAVE) {
        int master_fd = reactor_connect(master_ip, port);
        if (master_fd >= 0) {
            add_client_fd(master_fd);
            // register master_fd
            conn_list[master_fd].fd = master_fd;
            conn_list[master_fd].r_action.recv_callback = recv_cb;
            conn_list[master_fd].send_callback = send_cb;
            struct epoll_event ev;
            ev.data.fd = master_fd;
            ev.events = EPOLLIN;
            epoll_ctl(epfd, EPOLL_CTL_ADD, master_fd, &ev);
            reactor_broadcast(syncc, strlen(syncc));
            printf("[reactor] Connected to master (fd=%d)\n", master_fd);
        } else {
            fprintf(stderr, "[reactor] Failed to connect master\n");
        }
    }
#endif

    gettimeofday(&begin, NULL);

    // main loop
    while (1) {
        struct epoll_event events[1024];
        int n = epoll_wait(epfd, events, 1024, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("[reactor_start] epoll_wait");
            break;
        }

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            uint32_t evs = events[i].events;

            if (fd < 0 || fd >= CONNECTION_SIZE) continue;

            // If listen socket
            if (conn_list[fd].r_action.recv_callback == accept_cb && (evs & EPOLLIN)) {
                accept_cb(fd);
                continue;
            }

            if (evs & (EPOLLERR | EPOLLHUP)) {
                // error on fd
                close_connection(fd);
                continue;
            }

            if (evs & EPOLLIN) {
                if (conn_list[fd].r_action.recv_callback) conn_list[fd].r_action.recv_callback(fd);
            }

            if (evs & EPOLLOUT) {
                if (conn_list[fd].send_callback) conn_list[fd].send_callback(fd);
            }
        }
    }

    // cleanup on exit
    close(epfd);
    epfd = -1;
    return 0;
}
