#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <unistd.h>

#define MAX_MSG_LENGTH 4096
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

int send_msg(int connfd, char *msg, int length) {
    int res = send(connfd, msg, length, 0);
    if (res < 0) {
        perror("send");
        exit(1);
    }
    return res;
}

int recv_msg(int connfd, char *msg, int length) {
    int res = recv(connfd, msg, length, 0);
    if (res < 0) {
        perror("recv");
        exit(1);
    }
    return res;
}

int kvs_join_tokens(char *tokens[], int count, char *msg) {
    if (!tokens || !msg || count <= 0) return -1;
    char *p = msg;
    p += sprintf(p, "*%d\r\n", count);
    for (int i = 0; i < count; i++) {
        if (!tokens[i]) continue;
        int len = (int)strlen(tokens[i]);
        p += sprintf(p, "$%d\r\n", len);
        memcpy(p, tokens[i], len);
        p += len;
        memcpy(p, "\r\n", 2);
        p += 2;
    }
    *p = '\0';
    return p - msg;
}

int connect_tcpserver(const char *ip, unsigned short port) {
    int connfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(struct sockaddr_in));

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(ip);
    server_addr.sin_port = htons(port);

    if (0 != connect(connfd, (struct sockaddr*)&server_addr, sizeof(struct sockaddr_in))) {
        perror("connect");
        return -1;
    }
    return connfd;
}

void testcase(int connfd, char *tokens[], int count, char *pattern, char *casename) {
    if (!tokens || !pattern || !casename) return;
    char msg[MAX_MSG_LENGTH] = {0};
    kvs_join_tokens(tokens, count, msg);
    send_msg(connfd, msg, strlen(msg));
    char result[MAX_MSG_LENGTH] = {0};
    recv_msg(connfd, result, MAX_MSG_LENGTH);
    //printf("result: %s\n", result);
    if (strcmp(result, pattern) == 0) {
        //printf("==> PASS -> %s\n", casename);
    } else {
        printf("==> FAILED -> %s, '%s' != '%s'\n", casename, result, pattern);
        exit(1);
    }
}



void large_value_testcase_1w(int connfd,int count) {
    //const int count = 100000;
    int length=1000;
    // 构造 1000 字节的大 value
    char value[length+3];
    for (int i = 0; i < length; i++) {
        value[i] = 'A' + (i % 26);   // 循环生成 A-Z
    }
    //memcpy(value + length, "\r\n", 3);
    value[length] = '\0';
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    // 执行一万次
    for (int i = 0; i < count; i++) {

        // 构造 key，比如 K0, K1 ...
        char key[64];
        sprintf(key, "BigKey%d", i);

        char *cmd[] = {"LSET", key, value};

        // 调用你的 testcase，只检查 "OK\r\n"
        testcase(connfd, cmd, 3, "OK\r\n", "LSET-BigValue");
    }

    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin);
    int qps = count * 1000 / time_used;

    printf("large_value_set %d --> time_used: %d ms, qps: %d\n",
        count, time_used, qps);
}


//test_large_set 192.168.10.15 2000
int main(int argc, char *argv[]) {
    if (argc < 4) {
        printf("Usage: %s <ip> <port> <count>\n", argv[0]);
        //printf("mode: 0=rbtree_1w, 1=rbtree_3w, 2=array_1w, 3=hash_test\n");
        return -1;
    }

    char *ip = argv[1];
    int port = atoi(argv[2]);
    int count = atoi(argv[3]);

    int connfd = connect_tcpserver(ip, port);
    if (connfd < 0) return -1;

    large_value_testcase_1w(connfd,count);

    close(connfd);
    return 0;
}