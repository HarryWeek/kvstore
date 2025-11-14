



#include "nty_coroutine.h"
#include "kvstore.h"
#include "server.h"
#include <arpa/inet.h>
extern int client_fds[MAX_CLIENTS];
extern int client_count;
extern char *syncc;
typedef int (*msg_handler)(char *msg, int length, char *response);
static msg_handler kvs_handler;

static void add_client_fd(int fd) {
	for(int i=0;i<client_count;i++){
		if(client_fds[i]==fd) return;
	}
    if (client_count < MAX_CLIENTS) {
        client_fds[client_count++] = fd;
    }
}
void server_reader(void *arg) {
	int fd = *(int *)arg;
	int ret = 0;

 
	while (1) {
		
		char buf[BUFFER_LENGTH] = {0};
		ret = recv(fd, buf, BUFFER_LENGTH, 0);
		if (ret > 0) {
			if(strcmp(buf,syncc)==0){
				printf("get SYNC fd:%d\n",fd);
				add_client_fd(fd);
			}
			char response[1024] = {0};
			int slength = kvs_handler(buf, ret, response);

			ret = send(fd, response, slength, 0);
			if (ret == -1) {
				close(fd);
				break;
			}
		} else if (ret == 0) {	
			close(fd);
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
		

		nty_coroutine *read_co;
		nty_coroutine_create(&read_co, server_reader, &cli_fd);

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

    // 4. 加入 client_fds
    add_client_fd(sockfd);

    // 5. 注册协程 reader，模仿 proactor 的 set_event_recv
    nty_coroutine *read_co = NULL;
    nty_coroutine_create(&read_co, server_reader, &sockfd);

    printf("[ntyco] Connected to master %s:%d (fd=%d)\n",
           master_ip, conn_port, sockfd);

    return sockfd;
}



int ntyco_start(unsigned short port, msg_handler handler) {

	//int port = atoi(argv[1]);
	kvs_handler = handler;
#if ENABLE_MS

	extern char *master_ip;
	//extern unsigned short master_port;
	if(kvs_role==ROLE_SLAVE){
		printf("[proactor] Trying to connect master %s:%d ...\n", master_ip, port);
		int master_fd = ntyco_connect(master_ip, port);
		if (master_fd >= 0) {
			//add_client_fd(master_fd);
			printf("[proactor] Connected to master (fd=%d)\n", master_fd);
			
			ntyco_broadcast(syncc,strlen(syncc));
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
