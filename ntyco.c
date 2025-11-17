



#include "nty_coroutine.h"
#include "kvstore.h"
#include "server.h"
#include <arpa/inet.h>
extern int client_fds[MAX_CLIENTS];
extern int client_count;
extern char syncc[1024];
typedef int (*msg_handler)(char *msg, int length, char *response);
static msg_handler kvs_handler;

#define BUFFER_LENGTH 4096

static void remove_client_fd(int fd) {
	for (int i = 0; i < client_count; i++) {
		if (client_fds[i] == fd) {
			client_fds[i] = client_fds[client_count - 1];
			client_count--;
			break;
		}
	}
}

static void add_client_fd(int fd) {
	for(int i=0;i<client_count;i++){
		if(client_fds[i]==fd) return;
	}
    if (client_count < MAX_CLIENTS) {
        client_fds[client_count++] = fd;
    }
}
void server_reader(void *arg) {
	int *pfd = (int *)arg;
	int fd = *pfd;
	free(pfd);

	char rbuffer[BUFFER_LENGTH];
	int rlength = 0;
	int ret = 0;
	//printf("start server_reader fd:%d\n", fd);

	while (1) {
		ret = recv(fd, rbuffer + rlength, BUFFER_LENGTH - rlength, 0);
		//printf("recv fd:%d ret:%d\n", fd, ret);
		if (ret > 0) {
			rlength += ret;
			//printf("syncc:%s\n", syncc);
			if (rlength == (int)strlen(syncc) && memcmp(rbuffer, syncc, rlength) == 0) {
				printf("get SYNC fd:%d\n", fd);
				printf("get msg:%.*s\n", rlength, rbuffer);
				add_client_fd(fd);
				// consume the sync message
				//rlength = 0;
				//continue;
			}
			
			// 处理可能有多个完整包的缓冲区
			while (1) {
				int before_len = rlength;
				printf("get msg:%.*s\n", rlength, rbuffer);
				char *packet = parse_packet(rbuffer, &rlength, BUFFER_LENGTH);
				printf("packet parsed, length: %d\n", before_len - rlength);
				if (!packet) break; // 不完整包
				
				int packet_len = before_len - rlength;
				char response[BUFFER_LENGTH]={0};
				int slength = kvs_handler(packet, packet_len, response);
				//printf("slength:%d\n",slength);
				// 发送全部数据
				int sent_total = 0;
				while (sent_total < slength) {
					int s = send(fd, response + sent_total, slength - sent_total, 0);
					if (s <= 0) break;
					sent_total += s;
				}

				kvs_free(packet);
			}

			// 如果缓冲区已满且没有解析出完整包，直接清空以避免卡死
			if (rlength == BUFFER_LENGTH) rlength = 0;

		} else if (ret == 0) {
			close(fd);
			remove_client_fd(fd);
			break;
		} else {
			// recv 返回 -1
			close(fd);
			remove_client_fd(fd);
			break;
		}
	}
}



void server(void *arg) {

	unsigned short port = *(unsigned short *)arg;

	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) return ;

	struct sockaddr_in local, remote;
	local.sin_family = AF_INET;
	local.sin_port = htons(port);
	local.sin_addr.s_addr = INADDR_ANY;
	bind(fd, (struct sockaddr*)&local, sizeof(struct sockaddr_in));

	listen(fd, 20);
	printf("ntyco listen port : %d\n", port);


	while (1) {
		socklen_t len = sizeof(struct sockaddr_in);
		int cli_fd = accept(fd, (struct sockaddr*)&remote, &len);
		if (cli_fd < 0) continue;

		int *pfd = malloc(sizeof(int));
		if (!pfd) {
			close(cli_fd);
			continue;
		}
		*pfd = cli_fd;

		nty_coroutine *read_co;
		nty_coroutine_create(&read_co, server_reader, pfd);
	}
	
}


int ntyco_connect(char *master_ip, unsigned short conn_port) {
    if (!master_ip || conn_port == 0) {
        printf("[ntyco_connect] invalid param\n");
        return -1;
    }

    // 1. 创建 socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("[ntyco_connect] socket");
        return -1;
    }

    // 2. 目标地址
    struct sockaddr_in serveraddr;
    memset(&serveraddr, 0, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_port = htons(conn_port);

    if (inet_pton(AF_INET, master_ip, &serveraddr.sin_addr) <= 0) {
        perror("[ntyco_connect] inet_pton");
        close(sockfd);
        return -1;
    }

    // 3. connect
    if (connect(sockfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) < 0) {
        perror("[ntyco_connect] connect");
        close(sockfd);
        return -1;
    }


    // 5. 注册协程 reader，模仿 proactor 的 set_event_recv
	// 4. 加入 client_fds
	add_client_fd(sockfd);
	//ntyco_broadcast(syncc,strlen(syncc));
	// 5. 注册协程 reader，模仿 proactor 的 set_event_recv
	nty_coroutine *read_co = NULL;
	int *pfd = malloc(sizeof(int));
	if (!pfd) {
		close(sockfd);
		remove_client_fd(sockfd);
		return -1;
	}
	*pfd = sockfd;
	nty_coroutine_create(&read_co, server_reader, pfd);

    printf("[ntyco] Connected to master %s:%d (fd=%d)\n",
           master_ip, conn_port, sockfd);

    return sockfd;
}



int ntyco_start(unsigned short port, msg_handler handler) {

	//int port = atoi(argv[1]);
	kvs_handler = handler;
#if ENABLE_MS
	char *tokens[] = {"SYNCALL"};
    kvs_join_tokens(tokens, 1, syncc);
	//printf("syncc:%s\n", syncc);
	extern char *master_ip;
	//extern unsigned short master_port;
	if(kvs_role==ROLE_SLAVE){
		printf("[proactor] Trying to connect master %s:%d ...\n", master_ip, port);
		int master_fd = ntyco_connect(master_ip, port);
		if (master_fd >= 0) {
			//add_client_fd(master_fd);
			printf("[proactor] Connected to master (fd=%d)\n", master_fd);
			send(master_fd, syncc, strlen(syncc), 0);
			//ntyco_broadcast(syncc,strlen(syncc));
		} else {
			fprintf(stderr, "[proactor] Failed to connect master %s:%d\n", master_ip, port);
		}
		//proactor_broadcast("SYNC",4);
	}

#endif
	
	nty_coroutine *co = NULL;
	nty_coroutine_create(&co, server, &port);
	//printf("ntyco listen:%d\n",port);
	nty_schedule_run();

}




int ntyco_broadcast(const char *msg, size_t len) {
    if (!msg || len == 0) return -1;

    //printf("client_count: %d\n", client_count);
    if (client_count == 0) return -1;

    int send_count = 0;

    for (int i = 0; i < client_count; i++) {
        int fd = client_fds[i];
        if (fd <= 0) continue;

        ssize_t ret = send(fd, msg, len, 0);
        if (ret == -1) {
            //printf("send failed fd:%d errno:%d(%s)\n",
            //       fd, errno, strerror(errno));
            close(fd);
            client_fds[i] = -1; // 标记无效
            continue;
        }

        //printf("broadcast to fd:%d i:%d msg:%.*s\n",fd, i, (int)len, msg);
               

        send_count++;
    }

    //printf("[ntyco_broadcast] finished sending to %d clients\n", send_count);
    return send_count;
}
