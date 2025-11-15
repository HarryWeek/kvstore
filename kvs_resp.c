#include "kvstore.h"
#if 1
int max_tokens = 128;
int kvs_split_token(char *msg, char *tokens[]){
    if (!msg || !tokens) return -1;
    char *p = msg;
    if (*p != '#') return -1;
    p++;
    int body_len = atoi(p);   // RESP 内容长度
    char *rn = strstr(p, "\r\n");
    if (!rn) return -1;
    p = rn + 2;  // 跳过 "#num\r\n"
    const char *resp_start = p;  // RESP 开始位置

    // 2️⃣ 解析 RESP：先判断是否以 *
    if (*p != '*') return -1;

    p++;
    int arg_count = atoi(p);

    rn = strstr(p, "\r\n");
    if (!rn) return -1;
    p = rn + 2;

    int argc = 0;
    const int max_tokens = 128;

    // 3️⃣ 解析每个参数
    for (int i = 0; i < arg_count && argc < max_tokens; i++) {
        if (*p != '$') return -1;
        p++;

        int len = atoi(p);
        rn = strstr(p, "\r\n");
        if (!rn) return -1;
        p = rn + 2;

        tokens[argc] = malloc(len + 1);
        memcpy(tokens[argc], p, len);
        tokens[argc][len] = '\0';
        argc++;

        p += len + 2; // 跳过内容 + \r\n
    }

    // 4️⃣ 验证总长度是否一致
    if ((int)(p - resp_start) != body_len) {
        // RESP 内容长度不对应（说明发送端损坏）
        return -1;
    }

    return p - msg;  // 整个消息的总长度，包括表头
}

int kvs_join_tokens(char *tokens[], int count, char *msg) {
    if (!tokens || !msg || count <= 0) return -1;

    char resp_buf[8192];
    char *p = resp_buf;

    // 先写 RESP（不含表头）
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

    // 现在写入总包头 "#<len>\r\n"
    char *out = msg;
    out += sprintf(out, "#%d\r\n", resp_len);

    memcpy(out, resp_buf, resp_len);
    out += resp_len;

    *out = '\0';

    return out - msg;  // 整个消息长度
}

#endif
#if 0
int max_tokens=4;
int kvs_split_token(char *msg, char *tokens[]) {
	 if (!msg || !tokens) return -1;

    char *p = msg;
    int argc = 0;

    // 检查开头
    if (*p != '*') {
        //fprintf(stderr, "Invalid RESP: must start with '*' p:[%c]\n",*p);
        return -1;
    }
	//while(*p!='*'||*p!='\0') p++;
    //  读取参数个数
    p++;
    int arg_count = atoi(p);

    // 找到第一个 "\r\n"
    char *rn = strstr(p, "\r\n");
    if (!rn) return -1;
    p = rn + 2;  // 跳过 "\r\n"

    // 记录起始位置（用于计算总长度）
    const char *start = msg;

    // 3️⃣ 解析每个参数
    for (int i = 0; i < arg_count && argc < max_tokens; i++) {
        if (*p != '$') {
            //fprintf(stderr, "Invalid RESP: expected '$' before arg\n");
            return -1;
        }
        p++; // skip '$'

        int len = atoi(p);
        rn = strstr(p, "\r\n");
        if (!rn) return -1;

        p = rn + 2; // move to actual string

        tokens[argc] = (char*)malloc(len + 1);
        memcpy(tokens[argc], p, len);
        tokens[argc][len] = '\0';
        argc++;

        p += len + 2; // 跳过 data + \r\n
    }

    // 计算总长度
    int total_len = (int)(p - start);
    return total_len;
}
int kvs_join_tokens(char *tokens[], int count, char *msg) {
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
    return p-msg;
}
#endif

char* parse_packet(char *msg, int *msg_len, int buffer_size) {
    if (*msg_len <= 0) return NULL;

    if (*msg_len > buffer_size) {
        *msg_len = 0;
        return NULL;
    }

    int offset = 0;   // 当前解析位置
    int total_used = 0; // 完整包占用的总长度

    // 1️⃣ 先遍历所有完整包，计算总长度
    while (1) {
        if (*msg_len - offset < 4) break; // 不可能有完整包

        if (msg[offset] != '#') {
            *msg_len = 0; // 协议错误
            return NULL;
        }

        // 找 header 的 \r\n
        char *rn = memmem(msg + offset, *msg_len - offset, "\r\n", 2);
        if (!rn) break; // 半包

        int header_len = rn - (msg + offset) + 2;
        int body_len = atoi(msg + offset + 1);
        int packet_len = header_len + body_len;

        if (*msg_len - offset < packet_len) break; // 半包

        offset += packet_len; // 完整包长度累加
        total_used = offset;
    }

    // 没有完整包
    if (total_used == 0)
        return NULL;

    // 2️⃣ 分配 big full_packet 保存所有完整包
    char *full_packet = kvs_malloc(total_used + 1);
    memcpy(full_packet, msg, total_used);
    full_packet[total_used] = '\0';

    // 3️⃣ 缓冲区左移，保留半包
    int remain = *msg_len - total_used;
    if (remain > 0)
        memmove(msg, msg + total_used, remain);

    *msg_len = remain;

    return full_packet;  // 返回所有完整包
}

