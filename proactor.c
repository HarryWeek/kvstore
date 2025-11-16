#include "kvstore.h"
#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2
#define ENTRIES_LENGTH  4096
#define BUFFER_LENGTH   4096
#define MAX_CONN 1024
extern int kvs_protocol(char *msg, int length, char *response);
extern int kvs_ms_protocol(char *msg, int len, char *response);

#if ENABLE_MS
extern int client_fds[MAX_CLIENTS];
extern int client_count;
extern int slave_port;
extern int master_port;
extern char *master_ip;
extern struct io_uring ring;

int set_event_send(struct io_uring *ring, int sockfd,
                   void *buf, size_t len, int flags);
int set_event_recv(struct io_uring *ring, int sockfd,
                   void *buf, size_t len, int flags);

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

int proactor_broadcast(char *msg, size_t len) {
    if (client_count == 0) return -1;
    for (int i = 0; i < client_count; i++) {
        int fd = client_fds[i];
        set_event_send(&ring, fd, msg, len, 0);
        printf("send: %s\n",msg);
    }
    return client_count;
}


#endif


// =======================================================================
//      ★★★ 每个连接独立 buffer ★★★
// =======================================================================



struct connection {
    int fd;
    char buffer[BUFFER_LENGTH];
    int rlength;
};

static struct connection conn_list[MAX_CONN];
static int conn_used[MAX_CONN];

static char* get_conn_buffer(int fd) {
    for (int i = 0; i < MAX_CONN; i++) {
        if (conn_used[i] && conn_list[i].fd == fd) {
            return conn_list[i].buffer;
        }
    }
    return NULL;
}
int *get_conn_rlen(int fd){
    for(int i=0;i<MAX_CONN;i++){
        if(conn_used[i]&&conn_list[i].fd==fd){
            return &conn_list[i].rlength;
        }
    }
    return 0;
}


// =======================================================================
//                     原有 Proactor 核心代码
// =======================================================================

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
    struct conn_info info = { .fd = sockfd, .event = EVENT_READ };

    io_uring_prep_recv(sqe, sockfd, buf, len, flags);
    memcpy(&sqe->user_data, &info, sizeof(info));
    return 0;
}

int set_event_send(struct io_uring *ring, int sockfd,
                   void *buf, size_t len, int flags) {

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) return -1;

    struct conn_info info = { .fd = sockfd, .event = EVENT_WRITE };

    io_uring_prep_send(sqe, sockfd, buf, len, flags);
    memcpy(&sqe->user_data, &info, sizeof(info));

    int ret = io_uring_submit(ring);
    return ret > 0 ? 0 : -1;
}

int set_event_accept(struct io_uring *ring, int sockfd,
                     struct sockaddr *addr,
                     socklen_t *addrlen, int flags) {

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    struct conn_info info = { .fd = sockfd, .event = EVENT_ACCEPT };

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

    add_client_fd(sockfd);

    // static char recv_buf[BUFFER_LENGTH];
    // memset(recv_buf, 0, BUFFER_LENGTH);
                    for (int k = 0; k < MAX_CONN; k++) {
                    if (!conn_used[k]) {
                        conn_used[k] = 1;
                        conn_list[k].fd = sockfd;
                        memset(conn_list[k].buffer, 0, BUFFER_LENGTH);
                        conn_list[k].rlength=0;
                        break;
                    }
                }
                // =============================

                char* buf = get_conn_buffer(sockfd);
                set_event_recv(&ring, sockfd, buf, BUFFER_LENGTH, 0);
    //set_event_recv(&ring, sockfd, buf, BUFFER_LENGTH, 0);
    io_uring_submit(&ring);

    printf("[proactor] Connected to master %s:%d (fd=%d)\n", master_ip, conn_port, sockfd);

    return sockfd;
}
// =======================================================================
//                           主事件循环
// =======================================================================

extern char syncc[1024];
struct io_uring ring;

typedef int (*msg_handler)(char *msg, int length, char *response);
static msg_handler kvs_handler;

int proactor_start(unsigned short port, msg_handler handler) {

    int sockfd = p_init_server(port);
    printf("proactor listen port: %d\n", port);

    kvs_handler = handler;

    char *tokens[] = {"SYNCALL"};
    kvs_join_tokens(tokens, 1, syncc);

    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    if (io_uring_queue_init_params(ENTRIES_LENGTH, &ring, &params) < 0) {
        perror("io_uring_queue_init_params");
        return -1;
    }

    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);

    set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);

#if ENABLE_MS
    extern kvs_role_t kvs_role;

    if (kvs_role == ROLE_SLAVE) {
        printf("[proactor] Trying to connect master %s:%d ...\n", master_ip, port);
        int master_fd = proactor_connect(master_ip, port);

        if (master_fd > 0) {
            printf("add fd %d\n",master_fd);
            add_client_fd(master_fd);
            kvs_sync_msg(syncc, strlen(syncc));
        }
    }
#endif

    char response[BUFFER_LENGTH];

    while (1) {

        io_uring_submit(&ring);

        struct io_uring_cqe *cqes[128];
        int nready = io_uring_peek_batch_cqe(&ring, cqes, 128);

        for (int i = 0; i < nready; i++) {

            struct io_uring_cqe *entries = cqes[i];
            struct conn_info result;
            memcpy(&result, &entries->user_data, sizeof(result));

            // ==========================================
            //                ACCEPT
            // ==========================================
            if (result.event == EVENT_ACCEPT) {

                int connfd = entries->res;

                // ===== 创建独立 buffer =====
                for (int k = 0; k < MAX_CONN; k++) {
                    if (!conn_used[k]) {
                        conn_used[k] = 1;
                        conn_list[k].fd = connfd;
                        memset(conn_list[k].buffer, 0, BUFFER_LENGTH);
                        conn_list[k].rlength=0;
                        break;
                    }
                }
                // =============================

                char* buf = get_conn_buffer(connfd);
                set_event_recv(&ring, connfd, buf, BUFFER_LENGTH, 0);

                set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);
            }

            // ==========================================
            //                READ
            // ==========================================
            else if (result.event == EVENT_READ) {

                int ret = entries->res;

                if (ret <= 0) {
                    // 删除连接
                    for (int k = 0; k < MAX_CONN; k++) {
                        if (conn_used[k] && conn_list[k].fd == result.fd) {
                            conn_used[k] = 0;
                            break;
                        }
                    }
                    close(result.fd);
                    continue;
                }

                char* buf = get_conn_buffer(result.fd);
                int *rlen=get_conn_rlen(result.fd);
                if (*rlen + ret > BUFFER_LENGTH) {
                    // 缓冲区满了（极少），清空避免崩溃
                    *rlen = 0;
                }                
                //memcpy(buf + *rlen, buf, ret);
                *rlen += ret;
                buf[*rlen] = '\0'; 
                // 方便调试（不破坏 buffer）
                char tmp[BUFFER_LENGTH+1];
                memcpy(tmp, buf, *rlen);
                tmp[*rlen] = '\0';
                printf("get msg:%s rlen:%d\n", tmp, *rlen);
#if ENABLE_MS
                if (strcmp(buf, syncc) == 0) {
                    printf("get SYNC fd:%d\n",result.fd);
                    add_client_fd(result.fd);
                }
                
#endif
                //printf("get msg:%s rlen: %d\n",buf,*rlen);

                    char *packet=parse_packet(buf,rlen,BUFFER_LENGTH);
                    printf("get packet:%s\nrlennext:%d\n",packet,*rlen);
                    ret = kvs_handler(packet, strlen(packet), response);

                    set_event_send(&ring, result.fd, response, ret, 0);
                

            }

            // ==========================================
            //                WRITE
            // ==========================================
            else if (result.event == EVENT_WRITE) {
                struct connection *conn = NULL;
                for (int k = 0; k < MAX_CONN; k++) {
                    if (conn_used[k] && conn_list[k].fd == result.fd) {
                        conn = &conn_list[k];
                        break;
                    }
                }
                if (!conn) continue;
                //printf("fd:%d\n",result.fd);
                // 写完不清空 buffer，只继续读
                set_event_recv(&ring, result.fd,
                            conn->buffer + conn->rlength,
                            BUFFER_LENGTH - conn->rlength,
                            0);
     
            }
        }

        io_uring_cq_advance(&ring, nready);
    }

    return 0;
}
