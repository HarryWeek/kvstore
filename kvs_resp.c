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

    char resp_buf[4096];
    char *p = resp_buf;

    // 先写 RESP（不含表头）
    p += sprintf(p, "*%d\r\n", count);

    for (int i = 0; i < count; i++) {
        if(!tokens[i]) continue;
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
#if 1
#include <ctype.h>

// 返回：
//  - 已 alloc 的完整 packet 字节（以 '\0' 结尾），并把 *msg_len 设为剩余字节数
//  - 需要更多数据时返回 NULL（且 *msg_len 保持原值）
//  - 协议确凿错误时返回 NULL 并把 *msg_len 置为 0 （调用方可选择 close）
char* parse_packet(char *msg, int *msg_len, int buffer_size) {
    if (*msg_len <= 0) return NULL;

    int offset = 0;
    int total_used = 0;

    while (1) {
        // 需要至少 "#x\r\n" 的最简情况 (3 字节以上)
        if (*msg_len - offset < 3) break;

        // 如果当前位置不是 '#'，尝试找到下一个 '#'，丢弃前导垃圾但不全部清空
        if (msg[offset] != '#') {
            char *p = memchr(msg + offset, '#', *msg_len - offset);
            if (p) {
                int skip = p - (msg + offset);
                // 丢弃 skip 字节（前导垃圾）
                memmove(msg, msg + offset + skip, *msg_len - (offset + skip));
                *msg_len = *msg_len - (offset + skip);
                // 从头重新开始解析
                offset = 0;
                continue;
            } else {
                // 没有找到 '#'
                // 如果缓冲太大，认为是不可恢复的垃圾；否则等待更多数据
                if (*msg_len > buffer_size / 2) {
                    // 太多无用数据，直接作为协议错误
                    fprintf(stderr, "parse_packet: garbage with no '#', len=%d\n", *msg_len);
                    *msg_len = 0;
                    return NULL;
                } else {
                    // 等待更多数据
                    break;
                }
            }
        }

        // 找到 header 的 CRLF（在剩余全部长度内查找）
        int remaining = *msg_len - offset;
        char *rn = memmem(msg + offset, remaining, "\r\n", 2);
        if (!rn) {
            // header 还没完整到达，等待更多数据
            break;
        }

        int header_len = (rn - (msg + offset)) + 2;

        // 解析 body_len（从 offset+1 到 rn-1）
        int body_len = 0;
        // 手动解析，避免 atoi 在非 NUL 终止字符串上的不安全
        char *pnum = msg + offset + 1;
        char *pnum_end = rn;
        // 验证数字部分
        if (pnum >= pnum_end) {
            // header like "#\r\n" 错误
            *msg_len = 0;
            fprintf(stderr, "parse_packet: empty length in header\n");
            return NULL;
        }
        for (char *t = pnum; t < pnum_end; t++) {
            if (!isdigit((unsigned char)*t)) {
                *msg_len = 0;
                fprintf(stderr, "parse_packet: non-digit in length header\n");
                return NULL;
            }
            body_len = body_len * 10 + (*t - '0');
            if (body_len < 0) { // overflow check (defensive)
                *msg_len = 0;
                return NULL;
            }
        }

        if (body_len < 0) {
            *msg_len = 0;
            fprintf(stderr, "parse_packet: negative body length\n");
            return NULL;
        }
        if (body_len > buffer_size) {
            // body 超过缓冲能力：两个选择
            //  A) 报错并 close（这里我们返回协议错，把 *msg_len 置 0）
            //  B) 或者你可以选择扩容缓冲（这里采取 A）
            fprintf(stderr, "parse_packet: body_len %d > buffer_size %d\n", body_len, buffer_size);
            *msg_len = 0;
            return NULL;
        }

        int packet_len = header_len + body_len;
        if (*msg_len - offset < packet_len) {
            // 半包：body 未全部到达
            break;
        }

        // 已有一个完整 packet，继续循环以尝试解析更多连续包
        offset += packet_len;
        total_used = offset;
    }

    if (total_used == 0) {
        // 没有完整包
        return NULL;
    }

    // 复制完整连续数据块返回（保持与你原来行为一致）
    char *full_packet = kvs_malloc(total_used + 1);
    if (!full_packet) {
        fprintf(stderr, "parse_packet: malloc failed\n");
        *msg_len = 0;
        return NULL;
    }
    memcpy(full_packet, msg, total_used);
    full_packet[total_used] = '\0';

    int remain = *msg_len - total_used;
    if (remain > 0) {
        memmove(msg, msg + total_used, remain);
        msg[remain] = '\0';
    } else {
        msg[0] = '\0';
    }
    *msg_len = remain;
    return full_packet;
}

#endif
#if 0
char* parse_packet(char *msg, int *msg_len, int buffer_size) {
    if (*msg_len <= 0) return NULL;
    int offset = 0;
    int total_used = 0;
    //printf("msg: %s",msg);
    while (1) {
        if (*msg_len - offset < 4) break;

        if (msg[offset] != '#') {
            *msg_len = 0;
            printf("协议错误 %s\n", msg + offset);
            return NULL;
        }

        // 限制 header 查找范围（避免匹配 body 内的 CRLF）
        int search_len = (*msg_len - offset > 32) ? 32 : (*msg_len - offset);
        char *rn = memmem(msg + offset, search_len, "\r\n", 2);
        if (!rn) break;

        int header_len = rn - (msg + offset) + 2;
        int body_len = atoi(msg + offset + 1);

        if (body_len < 0 || body_len > buffer_size) {
            *msg_len = 0;
            printf("body 长度错误\n");
            return NULL;
        }

        int packet_len = header_len + body_len;
        if (*msg_len - offset < packet_len) break;

        offset += packet_len;
        total_used = offset;
    }

    if (total_used == 0)
        return NULL;

    char *full_packet = kvs_malloc(total_used + 1);
    memcpy(full_packet, msg, total_used);
    full_packet[total_used] = '\0';

    int remain = *msg_len - total_used;
    if (remain > 0) {
        memmove(msg, msg + total_used, remain);
        msg[remain] = '\0';
    } else {
        msg[0] = '\0';
    }
    *msg_len = remain;
    return full_packet;
}

#endif
#if 0
char* parse_packet(char *msg, int *msg_len, int buffer_size) {
    if (*msg_len <= 0) return NULL;
    //printf("msg: %s",msg);
    int offset = 0;
    int total_used = 0;

    while (1) {
        if (*msg_len - offset < 4) break;

        if (msg[offset] != '#') {
            *msg_len = 0;
            //printf("协议错误 %s\n",msg);
            return NULL;
        }

        // 限制 header 查找范围（避免匹配 body 内的 CRLF）
        int search_len = (*msg_len - offset > 32) ? 32 : (*msg_len - offset);
        char *rn = memmem(msg + offset, search_len, "\r\n", 2);
        if (!rn) break;

        int header_len = rn - (msg + offset) + 2;
        int body_len = atoi(msg + offset + 1);

        if (body_len < 0 || body_len > buffer_size) {
            *msg_len = 0;
            printf("body 长度错误\n");
            return NULL;
        }

        int packet_len = header_len + body_len;
        if (*msg_len - offset < packet_len) break;

        offset += packet_len;
        total_used = offset;
    }

    if (total_used == 0)
        return NULL;

    char *full_packet = kvs_malloc(total_used + 1);
    memcpy(full_packet, msg, total_used);
    full_packet[total_used] = '\0';

    int remain = *msg_len - total_used;
    if (remain > 0) {
        memmove(msg, msg + total_used, remain);
        msg[remain] = '\0';
    } else {
        msg[0] = '\0';
    }
    printf("remain:%d %s \n",remain,msg);
    *msg_len = remain;
    return full_packet;
}
#endif
#if 0
char* trans_parse_packet(char *msg, int *msg_len, int buffer_size) {
    if (*msg_len <= 0) return NULL;

    int offset = 0;
    int total_used = 0;

    while (1) {
        // 至少需要 5 字节(序号+类型) + "#x\r\n"(3字节)
        if (*msg_len - offset < 8) break;

        // 检查是否有序号和类型，不解析，只确保足够字节即可

        // '#': 必须在 offset+5 处
        if (msg[offset + 5] != '#') {
            // 和原逻辑一致：从 offset+5 后找 '#'
            //printf("get #\n");
            char *p = memchr(msg + offset + 5, '#', *msg_len - (offset + 5));
            if (p) {
                int skip = p - (msg + offset) - 5; // 跳过至 '#' 的距离
                memmove(msg, msg + offset + 5 + skip, *msg_len - (offset + 5 + skip));
                *msg_len -= (offset + 5 + skip);
                offset = 0;
                continue;
            } else {
                if (*msg_len > buffer_size / 2) {
                    fprintf(stderr, "parse_packet: garbage, no '#', len=%d\n", *msg_len);
                    *msg_len = 0;
                    return NULL;
                } else {
                    break;
                }
            }
        }

        // 解析 "#len\r\n"
        int header_off = offset + 5;       // header 从这里开始
        int remaining = *msg_len - header_off;

        char *rn = memmem(msg + header_off, remaining, "\r\n", 2);
        if (!rn) break; // header 未完整

        int header_len = (rn - (msg + header_off)) + 2; // 包括 CRLF

        // 解析数字 (从 "#xxxx" 中的 xxxx)
        int body_len = 0;
        char *pnum = msg + header_off + 1;
        char *pnum_end = rn;

        if (pnum >= pnum_end) {
            fprintf(stderr, "parse_packet: empty length\n");
            *msg_len = 0;
            return NULL;
        }

        for (char *t = pnum; t < pnum_end; t++) {
            if (!isdigit((unsigned char)*t)) {
                fprintf(stderr, "parse_packet: invalid digit\n");
                *msg_len = 0;
                return NULL;
            }
            body_len = body_len * 10 + (*t - '0');
        }

        if (body_len > buffer_size) {
            fprintf(stderr, "parse_packet: body_len %d > buffer %d\n", body_len, buffer_size);
            *msg_len = 0;
            return NULL;
        }

        // 包总长度 = 前导 5 字节 + header_len + body_len
        int packet_len = 5 + header_len + body_len;

        if (*msg_len - offset < packet_len) break; // 半包

        offset += packet_len;
        total_used = offset;
    }

    if (total_used == 0) return NULL;

    // 拷贝完整数据包块
    char *full_packet = kvs_malloc(total_used + 1);
    if (!full_packet) {
        fprintf(stderr, "parse_packet: malloc failed\n");
        *msg_len = 0;
        return NULL;
    }
    memcpy(full_packet, msg, total_used);
    full_packet[total_used] = '\0';

    // 移动剩余数据
    int remain = *msg_len - total_used;
    if (remain > 0) {
        memmove(msg, msg + total_used, remain);
        msg[remain] = '\0';
    } else {
        msg[0] = '\0';
    }
    *msg_len = remain+5;
    //printf("full packet: %s\n", full_packet);
    return full_packet;
}
#endif