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
    char resp_buf[8192];
    char *p = resp_buf;

    p += sprintf(p, "*%d\r\n", count);

    for (int i = 0; i < count; i++) {
        int len = strlen(tokens[i]);
        p += sprintf(p, "$%d\r\n", len);
        memcpy(p, tokens[i], len);
        p += len;
        memcpy(p, "\r\n", 2);
        p += 2;
    }

    int resp_len = p - resp_buf;

    char *out = msg;
    out += sprintf(out, "#%d\r\n", resp_len);
    memcpy(out, resp_buf, resp_len);
    out += resp_len;

    *out = '\0';
    return out - msg;
}

int connect_tcpserver(const char *ip, unsigned short port) {
    int connfd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(ip);
    server_addr.sin_port = htons(port);

    if (connect(connfd, (struct sockaddr*)&server_addr, sizeof(server_addr))) {
        perror("connect");
        return -1;
    }
    return connfd;
}

const char* get_prefix(const char *datatype) {
    if (strcmp(datatype, "rbtree") == 0) return "R";
    if (strcmp(datatype, "hash") == 0)   return "H";
    if (strcmp(datatype, "large") == 0)  return "L";
    return ""; // array
}

void check_response(const char* expected, const char* actual) {
    if (strcmp(expected, actual) != 0) {
        printf("\n[ERROR] Mismatch:\n");
        printf("Expected: %s", expected);
        printf("Actual:   %s\n", actual);
        exit(1);
    }
}

void send_case_and_check(int connfd, char *tokens[], int count, const char *expected) {
    char msg[MAX_MSG_LENGTH];
    kvs_join_tokens(tokens, count, msg);
    send_msg(connfd, msg, strlen(msg));

    char result[MAX_MSG_LENGTH] = {0};
    recv_msg(connfd, result, MAX_MSG_LENGTH);

    check_response(expected, result);
}

void run_test(int connfd, const char *datatype, const char *op, int count) {
    printf("Running %-6s %-6s  count=%d\n", datatype, op, count);

    const char *prefix = get_prefix(datatype);

    struct timeval t1, t2;
    gettimeofday(&t1, NULL);

    for (int i = 0; i < count; i++) {

        char key[64], val[128], cmd_name[32], expected[256];
        sprintf(key, "%s_K_%d", datatype, i);
        sprintf(val, "%s_V_%d", datatype, i);

        sprintf(cmd_name, "%s%s", prefix,
                (strcmp(op, "set") == 0 ? "SET" :
                 strcmp(op, "get") == 0 ? "GET" :
                 strcmp(op, "mod") == 0 ? "MOD" :
                 strcmp(op, "del") == 0 ? "DEL" :
                 "EXIST"));

        char *cmd[4];
        int argc = 0;

        if (strcmp(op, "set") == 0) {
            char *tmp[] = {cmd_name, key, val};
            memcpy(cmd, tmp, sizeof(tmp));
            argc = 3;
            sprintf(expected, "OK\r\n");
        }
        else if (strcmp(op, "get") == 0) {
            char *tmp[] = {cmd_name, key};
            memcpy(cmd, tmp, sizeof(tmp));
            argc = 2;
            sprintf(expected, "%s\r\n", val);
        }
        else if (strcmp(op, "mod") == 0) {
            sprintf(val, "MOD_%d", i);
            char *tmp[] = {cmd_name, key, val};
            memcpy(cmd, tmp, sizeof(tmp));
            argc = 3;
            sprintf(expected, "OK\r\n");
        }
        else if (strcmp(op, "del") == 0) {
            char *tmp[] = {cmd_name, key};
            memcpy(cmd, tmp, sizeof(tmp));
            argc = 2;
            sprintf(expected, "OK\r\n");
        }
        else if (strcmp(op, "exist") == 0) {
            char *tmp[] = {cmd_name, key};
            memcpy(cmd, tmp, sizeof(tmp));
            argc = 2;

            // 存在与否取决于当前 op 流程
            if (i < count) {
                sprintf(expected, "EXIST\r\n");   // 你也可以改成动态判断
            } else {
                sprintf(expected, "NO EXIST\r\n");
            }
        }
        else {
            printf("Unknown op: %s\n", op);
            return;
        }

        send_case_and_check(connfd, cmd, argc, expected);
    }

    gettimeofday(&t2, NULL);
    int ms = TIME_SUB_MS(t2, t1);
    printf("%s %s -> %d ops, %d ms, qps = %d\n\n",
           datatype, op, count, ms, count * 1000 / ms);
}

int main(int argc, char *argv[]) {
    if (argc < 6) {
        printf("Usage:\n");
        printf("  %s <ip> <port> <datatype> <op> <count>\n", argv[0]);
        printf("datatype: array | hash | rbtree | large\n");
        printf("op: set | get | mod | del | exist\n");
        return -1;
    }

    char *ip = argv[1];
    int port = atoi(argv[2]);
    char *datatype = argv[3];
    char *op = argv[4];
    int count = atoi(argv[5]);

    int connfd = connect_tcpserver(ip, port);
    if (connfd < 0) return -1;

    run_test(connfd, datatype, op, count);

    close(connfd);
    return 0;
}
