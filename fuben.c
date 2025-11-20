#include "kvstore.h"
#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>
#include <errno.h>

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2
#define ENTRIES_LENGTH  256
#define BUFFER_LENGTH   256
#define MAX_CONN 65535

extern int kvs_protocol(char *msg, int length, char *response);
extern int kvs_ms_protocol(char *msg, int len,char*response);
#if ENABLE_MS

extern int client_fds[MAX_CLIENTS];
extern int client_count;
extern int master_port;
extern int slave_port;
extern struct io_uring ring;  // 引用主loop的全局ring

#endif

/* ------------------ retransmit config ------------------ */
#define MAX_OUTSTANDING 64
#define MAX_RETRIES 5
#define INITIAL_RTO_MS 200

typedef enum { MSG_TYPE_DATA = 0, MSG_TYPE_ACK = 1 } msg_type_t;

typedef struct outstanding_s {
    uint32_t msg_id;      // 应用层消息ID
    char *buf;            // 指向已打包发送数据（header + payload），由发送方malloc
    int len;              // buf 长度
    int retries;          // 已重试次数
    long rto_ms;          // 当前 RTO (ms)
    struct timespec ts;   // 上次发送时间
    int pending;          // 是否仍未确认（1 = 未确认）
} outstanding_t;

typedef struct connection_s{
    int fd;
    char rbuffer[BUFFER_LENGTH];
    int rlength;
    char wbuffer[BUFFER_LENGTH];
    int wlength;
    int recv_pending;

    /* retransmit */
    outstanding_t outstanding[MAX_OUTSTANDING];
    int outstanding_count;
    uint32_t next_msg_id;
} connection_t;

connection_t p_conn_list[MAX_CONN];

struct io_uring ring;

/* -------------------- helpers -------------------- */
static inline void *pack_user_data(int fd, int event) {
    uintptr_t v = ((uintptr_t)fd << 2) | (uintptr_t)(event & 0x3);
    return (void *)v;
}
static inline void unpack_user_data(void *p, int *fd, int *event) {
    uintptr_t v = (uintptr_t)p;
    *event = (int)(v & 0x3);
    *fd = (int)(v >> 2);
}

static inline uint64_t now_ms() {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
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

/* 打包 header + payload，返回 malloc 的 buffer，len_out 返回长度 */
static char *pack_msg(uint32_t msg_id, msg_type_t type, const char *payload, int payload_len, int *len_out) {
    int total = 4 + 1 + payload_len;
    char *buf = malloc((size_t)total);
    if (!buf) return NULL;
    uint32_t id_net = htonl(msg_id);
    memcpy(buf, &id_net, 4);
    buf[4] = (char)type;
    if (payload_len > 0 && payload != NULL) memcpy(buf + 5, payload, (size_t)payload_len);
    if (len_out) *len_out = total;
    return buf;
}
static void unpack_header(const char *buf, uint32_t *msg_id, msg_type_t *type) {
    uint32_t id_net;
    memcpy(&id_net, buf, 4);
    *msg_id = ntohl(id_net);
    *type = (msg_type_t)buf[4];
}

/* -------------------- io_uring helpers (recv/send/accept) -------------------- */

int set_event_recv(struct io_uring *ring, int sockfd,
                      void *buf, size_t len, int flags) {

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        fprintf(stderr, "[set_event_recv] failed to get SQE: submission queue full\n");
        return -1;
    }

    io_uring_prep_recv(sqe, sockfd, buf, len, flags);
    io_uring_sqe_set_data(sqe, pack_user_data(sockfd, EVENT_READ));
    return 0;
}

int set_event_send(struct io_uring *ring, int sockfd,
                      void *buf, size_t len, int flags) {

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        fprintf(stderr, "[set_event_send] failed to get SQE: submission queue full\n");
        return -1;
    }

    /* make a short-lived copy into connection's wbuffer if small enough to avoid lifetime issues */
    void *send_ptr = buf;
    if (sockfd >= 0 && sockfd < MAX_CONN && len <= BUFFER_LENGTH) {
        connection_t *c = &p_conn_list[sockfd];
        if (c) {
            memcpy(c->wbuffer, buf, len);
            c->wlength = (int)len;
            send_ptr = c->wbuffer;
        }
    }

    io_uring_prep_send(sqe, sockfd, send_ptr, len, flags);
    io_uring_sqe_set_data(sqe, pack_user_data(sockfd, EVENT_WRITE));
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

    io_uring_prep_accept(sqe, sockfd, (struct sockaddr*)addr, addrlen, flags);
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
    int packed_len;
    char *packed = pack_msg(msg_id, MSG_TYPE_DATA, payload, payload_len, &packed_len);
    if (!packed) return -1;

    int idx = -1;
    for (int i = 0; i < MAX_OUTSTANDING; i++) {
        if (!conn->outstanding[i].pending) { idx = i; break; }
    }
    if (idx == -1) {
        free(packed);
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

    if (set_event_send(&ring, conn->fd, o->buf, o->len, 0) < 0) {
        fprintf(stderr, "[retransmit] send failed immediate for fd %d\n", conn->fd);
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
                    fprintf(stderr, "[retransmit] msg %u for fd %d exceed max retries, dropping\n", o->msg_id, c->fd);
                    free(o->buf);
                    o->buf = NULL;
                    o->pending = 0;
                    o->len = 0;
                    c->outstanding_count--;
                } else {
                    if (set_event_send(&ring, c->fd, o->buf, o->len, 0) < 0) {
                        fprintf(stderr, "[retransmit] failed to re-send for fd %d\n", c->fd);
                    }
                    o->retries++;
                    o->rto_ms *= 2;
                    if (o->rto_ms > 60000) o->rto_ms = 60000;
                    record_timespec_now(&o->ts);
                    fprintf(stderr, "[retransmit] retransmitted msg %u to fd %d retry=%d next_rto=%ld\n",
                            o->msg_id, c->fd, o->retries, o->rto_ms);
                }
            }
        }
    }
}

/* -------------------- utility client list -------------------- */
static void add_client_fd(int fd) {
#if ENABLE_MS
    for(int i=0;i<client_count;i++){
        if(client_fds[i]==fd) return;
    }
    if (client_count < MAX_CLIENTS) {
        client_fds[client_count++] = fd;
    }
#endif
}
static void remove_client_fd(int fd) {
#if ENABLE_MS
    for (int i = 0; i < client_count; i++) {
        if (client_fds[i] == fd) {
            client_fds[i] = client_fds[client_count - 1];
            client_count--;
            break;
        }
    }
#endif
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
#if ENABLE_MS
    if (client_count == 0) return -1;

    for (int i = 0; i < client_count; i++) {
        int fd = client_fds[i];
        connection_t *conn = &p_conn_list[fd];
        if (conn->fd <= 0) continue;
        send_with_retransmit(conn, msg, (int)len);
    }

    return client_count;
#else
    return -1;
#endif
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

/* -------------------- proactor start -------------------- */
extern char syncc[1024];
typedef int (*msg_handler)(char *msg, int length, char *response);
static msg_handler kvs_handler;

int proactor_start(unsigned short port, msg_handler handler) {
    int sockfd = p_init_server(port);
    if (sockfd < 0) return -1;

    /* init listening conn slot */
    connection_t *listen_conn = &p_conn_list[sockfd];
    listen_conn->fd = sockfd;
    listen_conn->rlength = 0;
    listen_conn->wlength = 0;
    listen_conn->recv_pending = 0;
    memset(listen_conn->rbuffer,0,BUFFER_LENGTH);
    memset(listen_conn->wbuffer,0,BUFFER_LENGTH);
    listen_conn->outstanding_count = 0;
    listen_conn->next_msg_id = 1;
    for (int i=0;i<MAX_OUTSTANDING;i++){ listen_conn->outstanding[i].pending = 0; listen_conn->outstanding[i].buf = NULL; }

    printf("proactor listen port: %d\n",port);
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

    if (set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0) < 0) {
        fprintf(stderr, "set_event_accept failed\n");
        io_uring_queue_exit(&ring);
        close(sockfd);
        return -1;
    }
    printf("Accept event set successfully\n");

#if ENABLE_MS
    extern char *master_ip;
    if(kvs_role==ROLE_SLAVE){
        printf("[proactor] Trying to connect master %s:%d ...\n", master_ip, port);
        // reuse proactor_connect from original code if present; otherwise user must call proactor_connect separately
    }
#endif

    char response[BUFFER_LENGTH] = {0};

    while (1) {
        io_uring_submit(&ring);

        struct io_uring_cqe *cqes[128];
        int nready = io_uring_peek_batch_cqe(&ring, cqes, 128);

        for (int i = 0; i < nready; i++) {
            struct io_uring_cqe *entries = cqes[i];
            int ev_fd, ev_event;
            unpack_user_data(io_uring_cqe_get_data(entries), &ev_fd, &ev_event);

            if (ev_event == EVENT_ACCEPT) {
                if (set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0) < 0) {
                    fprintf(stderr, "[proactor_start] set_event_accept failed\n");
                }

                int connfd = entries->res;
                if (connfd < 0) {
                    continue;
                }
                if (connfd >= MAX_CONN) { close(connfd); continue; }

                connection_t *conn = &p_conn_list[connfd];
                conn->fd = connfd;
                conn->rlength = 0;
                conn->wlength = 0;
                conn->recv_pending = 0;
                memset(conn->rbuffer, 0, BUFFER_LENGTH);
                memset(conn->wbuffer, 0, BUFFER_LENGTH);
                conn->outstanding_count = 0;
                conn->next_msg_id = 1;
                for (int j=0;j<MAX_OUTSTANDING;j++){ conn->outstanding[j].pending = 0; conn->outstanding[j].buf = NULL; }

                if (!conn->recv_pending) {
                    if (set_event_recv(&ring, connfd, conn->rbuffer + conn->rlength, BUFFER_LENGTH - conn->rlength, 0) < 0) {
                        fprintf(stderr, "[proactor_start] set_event_recv failed for fd %d\n", connfd);
                        close(connfd);
                        continue;
                    }
                    conn->recv_pending = 1;
                }

            } else if (ev_event == EVENT_READ) {
                int ret = entries->res;
                int result_fd = ev_fd;

                if (ret == 0) {
                    if (result_fd >=0 && result_fd < MAX_CONN) {
                        p_conn_list[result_fd].recv_pending = 0;
                    }
                    remove_client_fd(result_fd);
                    // cleanup outstanding
                    if (result_fd >= 0 && result_fd < MAX_CONN) {
                        connection_t *c = &p_conn_list[result_fd];
                        for (int j=0;j<MAX_OUTSTANDING;j++){
                            if (c->outstanding[j].pending) { free(c->outstanding[j].buf); c->outstanding[j].buf = NULL; c->outstanding[j].pending = 0; }
                        }
                        c->outstanding_count = 0;
                        close(result_fd);
                    }

                } else if (ret > 0) {
                    if (result_fd < 0 || result_fd >= MAX_CONN) { close(result_fd); continue; }
                    connection_t *conn2 = &p_conn_list[result_fd];
                    conn2->recv_pending = 0;

                    int old_len = conn2->rlength;
                    size_t add = (size_t)ret;
                    if (conn2->rlength + add > BUFFER_LENGTH) {
                        add = BUFFER_LENGTH - conn2->rlength;
                        printf("[proactor] Warning: buffer overflow for fd %d, truncating data\n", result_fd);
                    }
                    conn2->rlength += (int)add;
                    int new_len = conn2->rlength;

                    char *packet = parse_packet(conn2->rbuffer, &conn2->rlength, BUFFER_LENGTH);
                    if (!packet) {
                        printf("all msg: %s\n", conn2->rbuffer);
                        if (conn2->rlength == 0) {
                            remove_client_fd(result_fd);
                            close(result_fd);
                            conn2->recv_pending = 0;
                        } else {
                            // partial, wait for more
                        }
                    } else {
                        int packet_len = new_len - conn2->rlength; // header+payload length
                        if (packet_len < 5) {
                            // malformed
                            kvs_free(packet);
                        } else {
                            uint32_t msg_id; msg_type_t mtype;
                            unpack_header(packet, &msg_id, &mtype);
                            if (mtype == MSG_TYPE_ACK) {
                                // clear outstanding
                                connection_t *c = &p_conn_list[result_fd];
                                for (int k=0;k<MAX_OUTSTANDING;k++){
                                    if (c->outstanding[k].pending && c->outstanding[k].msg_id == msg_id) {
                                        free(c->outstanding[k].buf);
                                        c->outstanding[k].buf = NULL;
                                        c->outstanding[k].pending = 0;
                                        c->outstanding[k].len = 0;
                                        c->outstanding_count--;
                                        break;
                                    }
                                }
                            } else if (mtype == MSG_TYPE_DATA) {
                                int payload_len = packet_len - 5;
                                // call existing handler with payload (packet+5)
                                int resp_len = kvs_handler(packet + 5, payload_len, response);
                                // send ACK back
                                int ack_len; char ack_buf[5];
                                uint32_t id_net = htonl(msg_id);
                                memcpy(ack_buf, &id_net, 4);
                                ack_buf[4] = (char)MSG_TYPE_ACK;
                                set_event_send(&ring, result_fd, ack_buf, 5, 0);

                                // if you want to send response reliably, use send_with_retransmit
                                if (resp_len > 0) {
                                    send_with_retransmit(&p_conn_list[result_fd], response, resp_len);
                                }
                            }
                            kvs_free(packet);
                        }
                    }

                    size_t avail_after = BUFFER_LENGTH - conn2->rlength;
                    if (avail_after > 0) {
                        if (!conn2->recv_pending) {
                            if (set_event_recv(&ring, result_fd, conn2->rbuffer + conn2->rlength, avail_after, 0) < 0) {
                                fprintf(stderr, "[proactor] set_event_recv failed for fd %d after read\n", result_fd);
                                remove_client_fd(result_fd);
                                close(result_fd);
                                continue;
                            }
                            conn2->recv_pending = 1;
                        }
                    }
                }

            } else if (ev_event == EVENT_WRITE) {
                int ret = entries->res;
                int result_fd = ev_fd;
                if (result_fd < 0 || result_fd >= MAX_CONN) { close(result_fd); continue; }
                connection_t *conn3 = &p_conn_list[result_fd];
                size_t avail = BUFFER_LENGTH - conn3->rlength;
                if (avail == 0) {
                    printf("[proactor] Warning: buffer full after write for fd %d, resetting rlength\n", result_fd);
                    conn3->rlength = 0;
                    avail = BUFFER_LENGTH;
                }
                if (!conn3->recv_pending) {
                    if (set_event_recv(&ring, result_fd, conn3->rbuffer + conn3->rlength, avail, 0) < 0) {
                        fprintf(stderr, "[proactor_start] set_event_recv failed for fd %d after write\n", result_fd);
                        remove_client_fd(result_fd);
                        close(result_fd);
                        continue;
                    }
                    conn3->recv_pending = 1;
                }
            }
        }

        // handle retransmissions after processing CQEs
        check_retransmissions();

        io_uring_cq_advance(&ring, nready);
    }

}
