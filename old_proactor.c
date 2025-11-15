

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
#define ENTRIES_LENGTH		4096
#define BUFFER_LENGTH		4096
extern int kvs_protocol(char *msg, int length, char *response);
extern int kvs_ms_protocol(char *msg, int len,char*response);
#if ENABLE_MS

extern int client_fds[MAX_CLIENTS];
extern int client_count;
extern int master_port;
extern int slave_port;
extern struct io_uring ring;  // 引用主loop的全局ring

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

    // 注册recv事件，等主节点发数据
    static char recv_buf[BUFFER_LENGTH];
    set_event_recv(&ring, sockfd, recv_buf, BUFFER_LENGTH, 0);
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

	struct conn_info accept_info = {
		.fd = sockfd,
		.event = EVENT_READ,
	};
	
	io_uring_prep_recv(sqe, sockfd, buf, len, flags);
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));

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

	struct conn_info accept_info = {
		.fd = sockfd,
		.event = EVENT_ACCEPT,
	};
	
	io_uring_prep_accept(sqe, sockfd, (struct sockaddr*)addr, addrlen, flags);
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));

}


typedef int (*msg_handler)(char *msg, int length, char *response);
static msg_handler kvs_handler;



struct io_uring ring;
int proactor_start(unsigned short port, msg_handler handler) {
	//printf("proactor start\n");
	int sockfd = p_init_server(port);
	printf("proactor listen port: %d\n",port);
	kvs_handler = handler;
	char *sync="*1\r\n$7\r\nSYNCALL\r\n";
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
			kvs_sync_msg(sync,strlen(sync));
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

				set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);
				//printf("set_event_accept\n"); //

				int connfd = entries->res;

				set_event_recv(&ring, connfd, buffer, BUFFER_LENGTH, 0);

				
			} else if (result.event == EVENT_READ) {  //

				int ret = entries->res;

				if (ret == 0) {
					close(result.fd);

				} else if (ret > 0) {
#if ENABLE_MS
					if(strcmp(buffer,sync)==0){
						printf("get SYNC fd:%d\n",result.fd);
						add_client_fd(result.fd);
					}
#endif
					//int kvs_protocol(char *msg, int length, char *response);
					printf("get msg:%s\n",buffer);
					ret = kvs_handler(buffer, ret, response);

					
					set_event_send(&ring, result.fd, response, ret, 0);
				}
			}  else if (result.event == EVENT_WRITE) {
  //

				int ret = entries->res;
				//printf("set_event_send ret: %d, %s\n", ret, buffer);

				set_event_recv(&ring, result.fd, buffer, BUFFER_LENGTH, 0);
				
			}
			
		}

		io_uring_cq_advance(&ring, nready);
	}

}


