


#include <errno.h>
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <poll.h>
#include <sys/epoll.h>
#include <errno.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include "kvstore.h"
#include "server.h"


#define CONNECTION_SIZE			1024 // 1024 * 1024

#define MAX_PORTS			1

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)
int event_register(int fd, int event);
extern int client_fds[MAX_CLIENTS];
extern int client_count;

#if ENABLE_KVSTORE
static void add_client_fd(int fd);

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
        perror("[reactor_connect] connect");
        close(sockfd);
        return -1;
    }

    // 加入客户端数组（防止重复）
    //add_client_fd(sockfd);

    // 注册该连接到 epoll，监听 EPOLLIN 事件（等主节点发数据）
    //event_register(sockfd, EPOLLOUT);

    printf("[reactor_connect] Connected to master %s:%d (fd=%d)\n",
           master_ip, conn_port, sockfd);

    return sockfd;
}



typedef int (*msg_handler)(char *msg, int length, char *response);

static msg_handler kvs_handler;
char *syncc;
#if 0
int kvs_request(struct conn *c) {
	//printf("kvs_request\n");
	//c->rlength=strlen
	c->wlength = kvs_handler(c->rbuffer, c->rlength, c->wbuffer);
	//printf("c->wlength: %d c->rlength %d c->rbuffer %s\n",c->wlength,c->rlength,c->rbuffer);
	return c->wlength;
}
#endif
#if 1
int kvs_request(struct conn *c,char *packet,int pac_len) {
	//printf("kvs_request\n");
	//c->rlength=strlen
	c->wlength = kvs_handler(packet, pac_len, c->wbuffer);
	//printf("c->wlength: %d c->rlength %d c->rbuffer %s\n",c->wlength,c->rlength,c->rbuffer);
	return c->wlength;
}	
#endif
int kvs_response(struct conn *c) {
	return 0;
	

}


#endif



int accept_cb(int fd);
int recv_cb(int fd);
int send_cb(int fd);



int epfd = 0;
struct timeval begin;



struct conn conn_list[CONNECTION_SIZE] = {0};
// fd


int set_event(int fd, int event, int flag) {

	if (flag) {  // non-zero add

		struct epoll_event ev;
		ev.events = event;
		ev.data.fd = fd;
		epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);

	} else {  // zero mod

		struct epoll_event ev;
		ev.events = event;
		ev.data.fd = fd;
		epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
		
	}
	

}


int event_register(int fd, int event) {

	if (fd < 0) return -1;

	conn_list[fd].fd = fd;
	conn_list[fd].r_action.recv_callback = recv_cb;
	conn_list[fd].send_callback = send_cb;

	memset(conn_list[fd].rbuffer, 0, BUFFER_LENGTH);
	conn_list[fd].rlength = 0;

	memset(conn_list[fd].wbuffer, 0, BUFFER_LENGTH);
	conn_list[fd].wlength = 0;

	set_event(fd, event, 1);
}


// listenfd(sockfd) --> EPOLLIN --> accept_cb
int accept_cb(int fd) {

	struct sockaddr_in  clientaddr;
	socklen_t len = sizeof(clientaddr);

	int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
	if (clientfd < 0) {
		printf("accept errno: %d --> %s\n", errno, strerror(errno));
		return -1;
	}
	
	event_register(clientfd, EPOLLIN);  // | EPOLLET

	if ((clientfd % 1000) == 0) {

		struct timeval current;
		gettimeofday(&current, NULL);

		int time_used = TIME_SUB_MS(current, begin);
		memcpy(&begin, &current, sizeof(struct timeval));
		

		//printf("accept finshed: %d, time_used: %d\n", clientfd, time_used);

	}

	return 0;
}

#if 0
int recv_cb(int fd) {

	memset(conn_list[fd].rbuffer, 0, BUFFER_LENGTH );
	int count = recv(fd, conn_list[fd].rbuffer, BUFFER_LENGTH, 0);
	if (count == 0) { // disconnect
		//printf("client disconnect: %d\n", fd);
		close(fd);

		epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL); // unfinished

		return 0;
	} else if (count < 0) { // 

		printf("count: %d, errno: %d, %s\n", count, errno, strerror(errno));
		close(fd);
		epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);

		return 0;
	}

	
	conn_list[fd].rlength = count;
	//printf("RECV: %s\n", conn_list[fd].rbuffer);

#if 0 // echo

	conn_list[fd].wlength = conn_list[fd].rlength;
	memcpy(conn_list[fd].wbuffer, conn_list[fd].rbuffer, conn_list[fd].wlength);

	printf("[%d]RECV: %s\n", conn_list[fd].rlength, conn_list[fd].rbuffer);

#elif ENABLE_HTTP

	http_request(&conn_list[fd]);

#elif ENABLE_WEBSOCKET

	ws_request(&conn_list[fd]);

#elif ENABLE_KVSTORE
#if ENABLE_MS
	//printf("get msg:%s\n",conn_list[fd].rbuffer);
	if(strcmp(conn_list[fd].rbuffer,syncc)==0){
		printf("get SYNC fd:%d\n",fd);
		add_client_fd(fd);
	}
#endif
	kvs_request(&conn_list[fd]);

#endif


	set_event(fd, EPOLLOUT, 0);

	return count;
}
#endif
#if 1
int recv_cb(int fd) {

		// 假设 conn_list[fd].rbuffer 已经分配了足够的空间
	int count = recv(fd, conn_list[fd].rbuffer + conn_list[fd].rlength, BUFFER_LENGTH - conn_list[fd].rlength, 0);

	if (count == 0) {  // 客户端断开连接
		// printf("client disconnect: %d\n", fd);
		close(fd);
		epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL); // 从 epoll 中删除

		return 0;
	} else if (count < 0) {  // 接收错误
		printf("count: %d, errno: %d, %s\n", count, errno, strerror(errno));
		close(fd);
		epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);

		return 0;
	}

	// 更新 rlen 为新读取的数据长度
	conn_list[fd].rlength += count;

	//printf("RECV: %s\n", conn_list[fd].rbuffer);

#if 0 // echo

	conn_list[fd].wlength = conn_list[fd].rlength;
	memcpy(conn_list[fd].wbuffer, conn_list[fd].rbuffer, conn_list[fd].wlength);

	printf("[%d]RECV: %s\n", conn_list[fd].rlength, conn_list[fd].rbuffer);

#elif ENABLE_HTTP

	http_request(&conn_list[fd]);

#elif ENABLE_WEBSOCKET

	ws_request(&conn_list[fd]);

#elif ENABLE_KVSTORE

	char *packet=parse_packet(conn_list[fd].rbuffer,&conn_list[fd].rlength ,BUFFER_LENGTH);
#if ENABLE_MS
	printf("get msg:%s\n",packet);
	if(strcmp(packet,syncc)==0){
		printf("get SYNC fd:%d\n",fd);
		add_client_fd(fd);
	}
#endif
	int wlen=kvs_request(&conn_list[fd],packet,strlen(packet));
	//printf("wlen:%d\n",wlen);
#endif


	set_event(fd, EPOLLOUT, 0);

	return count;
}

#endif
#if 0
int send_cb(int fd) {

#if ENABLE_HTTP

	http_response(&conn_list[fd]);

#elif ENABLE_WEBSOCKET

	ws_response(&conn_list[fd]);

#elif ENABLE_KVSTORE

	kvs_response(&conn_list[fd]);

#endif

	int count = 0;

#if 0
	if (conn_list[fd].status == 1) {
		//printf("SEND: %s\n", conn_list[fd].wbuffer);
		count = send(fd, conn_list[fd].wbuffer, conn_list[fd].wlength, 0);
		set_event(fd, EPOLLOUT, 0);
	} else if (conn_list[fd].status == 2) {
		set_event(fd, EPOLLOUT, 0);
	} else if (conn_list[fd].status == 0) {

		if (conn_list[fd].wlength != 0) {
			count = send(fd, conn_list[fd].wbuffer, conn_list[fd].wlength, 0);
		}
		
		set_event(fd, EPOLLIN, 0);
	}
#else
	//printf("sendcb: %s\n",conn_list[fd].wbuffer);
	if (conn_list[fd].wlength > 0) {
		count = send(fd, conn_list[fd].wbuffer, conn_list[fd].wlength, 0);
	}
	
	set_event(fd, EPOLLIN, 0);

#endif
	//set_event(fd, EPOLLOUT, 0);

	return count;
}
#endif

int send_cb(int fd) {
    struct conn *c = &conn_list[fd];
    if (c->wlength == 0) {
        // 没数据发，恢复只监听读
        set_event(fd, EPOLLIN, 0);
        return 0;
    }

    int sent = send(fd, c->wbuffer, c->wlength, 0);
	printf("send: %s\n",c->wbuffer);
    if (sent > 0) {
        if (sent < c->wlength) {
            // 还有剩余数据没发完
            memmove(c->wbuffer, c->wbuffer + sent, c->wlength - sent);
            c->wlength -= sent;
        } else {
            // 全发完了
            c->wlength = 0;
        }
    } else if (sent < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // 系统缓冲区满，下次再发
        } else {
            // 出错 -> 删除连接
            fprintf(stderr, "[send_cb] fd=%d send failed (%s)\n", fd, strerror(errno));
            close(fd);
            epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
            return -1;
        }
    }

    // 根据是否还有未发完数据决定是否保留EPOLLOUT
    struct epoll_event ev;
    ev.data.fd = fd;
    ev.events = EPOLLIN | (c->wlength > 0 ? EPOLLOUT : 0);
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);

    return sent;
}




int r_init_server(unsigned short port) {

	int sockfd = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in servaddr;
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY); // 0.0.0.0
	servaddr.sin_port = htons(port); // 0-1023, 

	if (-1 == bind(sockfd, (struct sockaddr*)&servaddr, sizeof(struct sockaddr))) {
		printf("bind failed: %s\n", strerror(errno));
	}

	listen(sockfd, 10);
	//printf("listen finshed: %d\n", sockfd); // 3 
	printf("reactor listen:%d\n",port);
	return sockfd;

}
#if 1
int reactor_broadcast(char *msg, size_t len) {
    if (!msg || len == 0) return -1;
    if (client_count == 0) return -1;

    int send_count = 0;

    for (int i = 0; i < client_count; i++) {
        int fd = client_fds[i];
        if (fd <= 0) continue;

        size_t used = conn_list[fd].wlength;
        size_t remain = BUFFER_LENGTH - used;

        if (remain <= 1) {
            // 缓冲区满，暂时跳过该客户端
            //fprintf(stderr, "[reactor_broadcast] Buffer full for fd=%d, skip\n", fd);
            continue;
        }

        // 预留 1 字节给 '\0'
        size_t copylen = (len > (remain - 1)) ? (remain - 1) : len;

        memcpy(conn_list[fd].wbuffer + used, msg, copylen);
        conn_list[fd].wlength = used + copylen;
        conn_list[fd].wbuffer[conn_list[fd].wlength] = '\0';

        // 注册可写事件
        set_event(fd, EPOLLOUT, 0);

        // 尝试立即发送一次（非阻塞 send）
        send_cb(fd);

        send_count++;
    }

    return send_count;
}

#endif
#if 0
int reactor_broadcast(const char *msg, size_t len) {
    if (!msg || len == 0) return -1;

    if (client_count == 0) return -1;

    int send_count = 0;

    for (int i = 0; i < client_count; i++) {
        int fd = client_fds[i];
        if (fd <= 0) continue;
		//printf("send:%s\n",msg);
        size_t used = conn_list[fd].wlength;
        size_t remain = BUFFER_LENGTH - used;

        size_t copylen;

       
            // 缓冲区已满：覆盖写入
            //fprintf(stderr,
               // "[reactor_broadcast] Buffer full, overwriting for fd=%d\n", fd);

            // 重置
            used = 0;
            conn_list[fd].wlength = 0;
            remain = BUFFER_LENGTH;
            //memset(conn_list[fd].wbuffer,0,sizeof(conn_list[fd].wbuffer));

        // 预留 1 字节做 '\0'
        copylen = (len > (remain - 1)) ? (remain - 1) : len;

        memcpy(conn_list[fd].wbuffer + used, msg, copylen);
        conn_list[fd].wlength = used + copylen;
        conn_list[fd].wbuffer[conn_list[fd].wlength] = '\0';

        // 注册发送事件
        set_event(fd, EPOLLOUT, 0);

        // 立刻尝试发送一次
        send_cb(fd);

        send_count++;
    }

    //printf("[reactor_broadcast] finished sending to %d clients\n", send_count);
    return send_count;
}
#endif
#if 0
int reactor_broadcast(const char *msg, size_t len) {
    if (!msg || len == 0) return -1;
    if (client_count == 0) return -1;

    int send_count = 0;

    for (int i = 0; i < client_count; i++) {
        int fd = client_fds[i];
        if (fd <= 0) continue;

        struct conn *c = &conn_list[fd];
        if (c->fd <= 0) continue;

        // 写缓冲剩余空间
        size_t remain = BUFFER_LENGTH - c->wlength;
        if (remain <= len + 1) {
            fprintf(stderr, "[reactor_broadcast] fd=%d buffer full, skip message\n", fd);
            continue;
        }

        // 追加数据
        memcpy(c->wbuffer + c->wlength, msg, len);
        c->wlength += len;
        c->wbuffer[c->wlength] = '\0';

        // 注册EPOLLOUT事件（非覆盖）
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLOUT;  // 读写都监听
        ev.data.fd = fd;
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);

        send_count++;
        if (c->wlength > BUFFER_LENGTH * 0.8) {
            usleep(1000); // 1ms
        }		
    }

    return send_count;
}


#endif

static void add_client_fd(int fd) {
	for(int i=0;i<client_count;i++){
		if(client_fds[i]==fd) return;
	}
    if (client_count < MAX_CLIENTS) {
        client_fds[client_count++] = fd;
		//int sockfd = r_init_server(port + i);
		
		//conn_list[fd].fd = fd;
		//conn_list[fd].r_action.recv_callback = accept_cb;
		//conn_list[fd].fd = fd;
		//conn_list[fd].r_action.recv_callback = recv_cb;
		//conn_list[fd].send_callback = send_cb;
		//set_event(fd, EPOLLIN, 1);
    }
}



int reactor_start(unsigned short port, msg_handler handler) {

	//unsigned short port = 2000;
	kvs_handler = handler;


	epfd = epoll_create(1);

	int i = 0;

	for (i = 0;i < MAX_PORTS;i ++) {
		
		int sockfd = r_init_server(port + i);
		
		conn_list[sockfd].fd = sockfd;
		conn_list[sockfd].r_action.recv_callback = accept_cb;
		
		set_event(sockfd, EPOLLIN, 1);
	}
#if ENABLE_MS

	extern char *master_ip;
	//extern unsigned short master_port;
	if(kvs_role==ROLE_SLAVE){
		printf("[reactor] Trying to connect master %s:%d ...\n", master_ip, port);
		int master_fd = reactor_connect(master_ip, port);
		if (master_fd >= 0) {
			add_client_fd(master_fd);
			event_register(master_fd,EPOLLIN);
			printf("[reactor] Connected to master (fd=%d)\n", master_fd);
			char *tokens[] = {"SYNCALL"};
			kvs_join_tokens(tokens,1,syncc);
			reactor_broadcast(syncc,strlen(syncc));
		} else {
			fprintf(stderr, "[reactor] Failed to connect master %s:%d\n", master_ip, port);
		}
		//proactor_broadcast("SYNC",4);
	}

#endif
	gettimeofday(&begin, NULL);

	while (1) { // mainloop

		struct epoll_event events[1024] = {0};
		int nready = epoll_wait(epfd, events, 1024, -1);

		int i = 0;
		for (i = 0;i < nready;i ++) {

			int connfd = events[i].data.fd;

#if 0
			if (events[i].events & EPOLLIN) {
				conn_list[connfd].r_action.recv_callback(connfd);
			} else if (events[i].events & EPOLLOUT) {
				conn_list[connfd].send_callback(connfd);
			}

#else 
			if (events[i].events & EPOLLIN) {
				conn_list[connfd].r_action.recv_callback(connfd);
			} 

			if (events[i].events & EPOLLOUT) {
				conn_list[connfd].send_callback(connfd);
			}
#endif
		}

	}
	

}


