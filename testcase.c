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
    printf("result: %s\n", result);
    if (strcmp(result, pattern) == 0) {
        printf("==> PASS -> %s\n", casename);
    } else {
        printf("==> FAILED -> %s, '%s' != '%s'\n", casename, result, pattern);
        //exit(1);
    }
}

/* ---------------- Array ---------------- */
void array_testcase(int connfd) {
    char *cmd1[] = {"SET", "Teacher", "King"};
    testcase(connfd, cmd1, 3, "OK\r\n", "SET-Teacher");

    char *cmd2[] = {"GET", "Teacher"};
    testcase(connfd, cmd2, 2, "King\r\n", "GET-Teacher");

    char *cmd3[] = {"MOD", "Teacher", "Darren"};
    testcase(connfd, cmd3, 3, "OK\r\n", "MOD-Teacher");

    testcase(connfd, cmd2, 2, "Darren\r\n", "GET-Teacher");

    char *cmd4[] = {"EXIST", "Teacher"};
    testcase(connfd, cmd4, 2, "EXIST\r\n", "EXIST-Teacher");

    char *cmd5[] = {"DEL", "Teacher"};
    testcase(connfd, cmd5, 2, "OK\r\n", "DEL-Teacher");

    testcase(connfd, cmd2, 2, "NO EXIST\r\n", "GET-Teacher");
}

/* ---------------- Rbtree ---------------- */
void rbtree_testcase(int connfd) {
    char *cmd1[] = {"RSET", "Teacher", "King"};
    testcase(connfd, cmd1, 3, "OK\r\n", "RSET-Teacher");

    char *cmd2[] = {"RGET", "Teacher"};
    testcase(connfd, cmd2, 2, "King\r\n", "RGET-Teacher");

    char *cmd3[] = {"RMOD", "Teacher", "Darren"};
    testcase(connfd, cmd3, 3, "OK\r\n", "RMOD-Teacher");

    testcase(connfd, cmd2, 2, "Darren\r\n", "RGET-Teacher");

    char *cmd4[] = {"REXIST", "Teacher"};
    testcase(connfd, cmd4, 2, "EXIST\r\n", "REXIST-Teacher");

    char *cmd5[] = {"RDEL", "Teacher"};
    testcase(connfd, cmd5, 2, "OK\r\n", "RDEL-Teacher");

    testcase(connfd, cmd2, 2, "NO EXIST\r\n", "RGET-Teacher");
}

/* ---------------- Hash ---------------- */
void hash_testcase(int connfd) {
    char *cmd1[] = {"HSET", "Teacher", "King"};
    testcase(connfd, cmd1, 3, "OK\r\n", "HSET-Teacher");

    char *cmd2[] = {"HGET", "Teacher"};
    testcase(connfd, cmd2, 2, "King\r\n", "HGET-Teacher");

    char *cmd3[] = {"HMOD", "Teacher", "Darren"};
    testcase(connfd, cmd3, 3, "OK\r\n", "HMOD-Teacher");

    testcase(connfd, cmd2, 2, "Darren\r\n", "HGET-Teacher");

    char *cmd4[] = {"HEXIST", "Teacher"};
    testcase(connfd, cmd4, 2, "EXIST\r\n", "HEXIST-Teacher");

    char *cmd5[] = {"HDEL", "Teacher"};
    testcase(connfd, cmd5, 2, "OK\r\n", "HDEL-Teacher");

    testcase(connfd, cmd2, 2, "NO EXIST\r\n", "HGET-Teacher");
}
void lagre_testcase(int connfd) {
    char *cmd1[] = {"LSET", "Teacher", "King"};
    testcase(connfd, cmd1, 3, "OK\r\n", "LSET-Teacher");

    char *cmd2[] = {"LGET", "Teacher"};
    testcase(connfd, cmd2, 2, "King\r\n", "LGET-Teacher");

    char *cmd3[] = {"LMOD", "Teacher", "Darren"};
    testcase(connfd, cmd3, 3, "OK\r\n", "LMOD-Teacher");

    testcase(connfd, cmd2, 2, "Darren\r\n", "LGET-Teacher");

    char *cmd4[] = {"LEXIST", "Teacher"};
    testcase(connfd, cmd4, 2, "EXIST\r\n", "LEXIST-Teacher");

    char *cmd5[] = {"LDEL", "Teacher"};
    testcase(connfd, cmd5, 2, "OK\r\n", "LDEL-Teacher");

    testcase(connfd, cmd2, 2, "NO EXIST\r\n", "LGET-Teacher");
}
/* ---------------- 性能测试 ---------------- */
void rbtree_testcase_1w(int connfd) {
    int count = 1000;
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {
        char key[32], val[32];
        sprintf(key, "K%d", i);
        sprintf(val, "V%d", i);
        char *cmd1[] = {"RSET", key, val};
        testcase(connfd, cmd1, 3, "OK\r\n", "RSET");
    }

    gettimeofday(&tv_end, NULL);
    int time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("rbtree_1w %d--> time_used: %d ms, qps: %d\n", count,time_used, count * 1000 / time_used);
}

void rbtree_get_testcase_1w(int connfd) {
    int count = 10000;
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {
        char key[32], val[32];
        sprintf(key, "K%d", i);
        //sprintf(val, "V%d\r\n", i);
        char *cmd1[] = {"RGET", key};
        testcase(connfd, cmd1, 2, "", "RGET");
    }

    gettimeofday(&tv_end, NULL);
    int time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("rbtree_1w %d--> time_used: %d ms, qps: %d\n", count,time_used, count * 1000 / time_used);
}


void large_value_testcase_1w(int connfd) {
    const int count = 100000;

    // 构造 1000 字节的大 value
    char value[1001];
    for (int i = 0; i < 3000; i++) {
        value[i] = 'A' + (i % 26);   // 循环生成 A-Z
    }
    value[1000] = '\0';

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

    printf("large_value_1w %d --> time_used: %d ms, qps: %d\n",
        count, time_used, qps);
}

void large_value_get_testcase_1w(int connfd) {
    const int count = 100000;

    // 构造 1000 字节的大 value
    char value[1001];
    for (int i = 0; i < 900; i++) {
        value[i] = 'A' + (i % 26);   // 循环生成 A-Z
    }
    value[1000] = '\0';

    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    // 执行一万次
    for (int i = 0; i < count; i++) {

        // 构造 key，比如 K0, K1 ...
        char key[64];
        sprintf(key, "BigKey%d", i);

        char *cmd[] = {"LGET", key};

        // 调用你的 testcase，只检查 "OK\r\n"
        testcase(connfd, cmd, 2, "OK\r\n", "LGET-BigValue");
    }

    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin);
    int qps = count * 1000 / time_used;

    printf("large_value_1w %d --> time_used: %d ms, qps: %d\n",
        count, time_used, qps);
}

void hash_testcase_1w(int connfd) {
    int count = 10000;
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {
        char key[32], val[32];
        sprintf(key, "K%d", i);
        sprintf(val, "V%d", i);
        char *cmd1[] = {"HSET", key, val};
        testcase(connfd, cmd1, 3, "OK\r\n", "HSET");
    }

    gettimeofday(&tv_end, NULL);
    int time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("hash_1w %d--> time_used: %d ms, qps: %d\n", count,time_used, count * 1000 / time_used);
}
void hash_get_testcase_1w(int connfd) {
    int count = 10000;
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {
        char key[32], val[32];
        sprintf(key, "K%d", i);
        sprintf(val, "V%d\r\n", i);
        char *cmd1[] = {"HSET", key};
        testcase(connfd, cmd1, 2, val, "HSET");
    }

    gettimeofday(&tv_end, NULL);
    int time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("hash_1w %d--> time_used: %d ms, qps: %d\n", count,time_used, count * 1000 / time_used);
}
void array_testcase_1w(int connfd) {
    int count = 10000;
    printf("array test\n");
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {
        char key[32], val[32];
        sprintf(key, "K%d", i);
        sprintf(val, "V%d", i);
        char *cmd1[] = {"SET", key, val};
        testcase(connfd, cmd1, 3, "OK\r\n", "SET");
    }

    gettimeofday(&tv_end, NULL);
    int time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("array_1w %d--> time_used: %d ms, qps: %d\n", count,time_used, count * 1000 / time_used);
}
void array_get_testcase_1w(int connfd) {
    int count = 10000;
    printf("array test\n");
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {
        char key[32], val[32];
        sprintf(key, "K%d", i);
        sprintf(val, "V%d\r\n", i);
        char *cmd1[] = {"GET", key};
        testcase(connfd, cmd1, 2, val, "GET");
    }

    gettimeofday(&tv_end, NULL);
    int time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("array_1w %d--> time_used: %d ms, qps: %d\n", count,time_used, count * 1000 / time_used);
}
#if 1
/* ---------------- 主函数 ---------------- */
int main(int argc, char *argv[]) {
    if (argc < 4) {
        printf("Usage: %s <ip> <port> <mode>\n", argv[0]);
        printf("mode: 0=rbtree_1w, 1=rbtree_3w, 2=array_1w, 3=hash_test\n");
        return -1;
    }

    char *ip = argv[1];
    int port = atoi(argv[2]);
    int mode = atoi(argv[3]);

    int connfd = connect_tcpserver(ip, port);
    if (connfd < 0) return -1;

    if (mode == 0) {
        rbtree_testcase_1w(connfd);
    } else if (mode == 1) {
        rbtree_testcase(connfd);
    } else if (mode == 2) {
        array_testcase(connfd);
    } else if (mode == 3) {
        hash_testcase(connfd);
    }else if (mode  == 4){
		lagre_testcase(connfd);
	} else if (mode == 6) {
        large_value_testcase_1w(connfd);
    }else if (mode ==7){
        hash_testcase_1w(connfd);
    }else if(mode==8){
        hash_get_testcase_1w(connfd);
    }else if (mode==9){
        array_testcase_1w(connfd);
    }else if (mode ==10){
        rbtree_get_testcase_1w(connfd);
    }else if(mode ==11){
        array_get_testcase_1w(connfd);
    }else if(mode == 12){
        large_value_get_testcase_1w(connfd);
    }
	else {
        printf("Unknown mode\n");
    }

    close(connfd);
    return 0;
}
#endif 
#if 0
// testcase 192.168.10.15 2000
int main(int argc, char *argv[]) {

    // if (argc != 3) {
    //     printf("arg error\n");
    //     return -1;
    // }

    char *ip = argv[1];
    int port = atoi(argv[2]);
    //int mode = atoi(argv[3]);

    char *tokens[4] = {0};
    tokens[0] = argv[3];
    tokens[1] = argv[4];
    tokens[2] = argv[5];

    int count = 3;
    if (tokens[2] == NULL) count = 2;

    //char *msg="SYNCALL";
	//char *tokens[1024];
	//tokens[0]=msg;
    //kvs_join_tokens(tokens, count, msg);

    int connfd = connect_tcpserver(ip, port);
    testcase(connfd,tokens,count,"OK","SYNC test");

    return 0;
}
#endif
