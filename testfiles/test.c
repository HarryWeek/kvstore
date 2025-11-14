#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

int parse_resp_tokens(char *msg, char *tokens[], int max_tokens) {
    if (!msg || !tokens) return -1;

    char *p = msg;
    int argc = 0;

    // 1. 检查第一个字符是否为 '*'
    if (*p != '*') {
        fprintf(stderr, "Invalid RESP: must start with '*'\n");
        return -1;
    }

    // 2. 读取参数个数
    p++;  // skip '*'
    int arg_count = atoi(p);

    // 跳过直到第一个 "\r\n"
    char *rn = strstr(p, "\r\n");
    if (!rn) return -1;
    p = rn + 2;  // move past "\r\n"

    // 3. 循环解析每个参数
    for (int i = 0; i < arg_count && argc < max_tokens; i++) {
        if (*p != '$') {
            fprintf(stderr, "Invalid RESP: expected '$' before arg\n");
            return -1;
        }
        p++; // skip '$'

        int len = atoi(p);
        rn = strstr(p, "\r\n");
        if (!rn) return -1;

        p = rn + 2; // move to actual string

        // 提取参数字符串
        tokens[argc] = (char*)malloc(len + 1);
        memcpy(tokens[argc], p, len);
        tokens[argc][len] = '\0';
        argc++;

        p += len + 2; // skip <data>\r\n
    }

    return argc;
}

int build_resp_message(char *tokens[], int count, char *msg) {
    if (!tokens || !msg || count <= 0) return -1;

    char *p = msg;

    // 1️⃣ 写入参数个数
    p += sprintf(p, "*%d\r\n", count);

    // 2️⃣ 写入每个参数的 $len + 内容
    for (int i = 0; i < count; i++) {
        if (!tokens[i]) continue;
        int len = (int)strlen(tokens[i]);
        p += sprintf(p, "$%d\r\n", len);
        memcpy(p, tokens[i], len);
        p += len;
        memcpy(p, "\r\n", 2);
        p += 2;
    }

    *p = '\0'; // 结束符
    return 0;
}
int main() {
    char *msg = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n";
    char *tokens[10] = {0};

    int count = parse_resp_tokens(msg, tokens, 10);

    printf("Parsed %d tokens:\n", count);
    for (int i = 0; i < count; i++) {
        printf("  tokens[%d] = '%s'\n", i, tokens[i]);
        //free(tokens[i]); // 别忘释放内存
    }
    char *resp=malloc(1024);
    build_resp_message(tokens,count,resp);
    printf("response: %s\n",resp);
    return 0;
}
