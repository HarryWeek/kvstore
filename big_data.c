#include"kvstore.h"
int kvs_large_set(char *key, char *value) {
    if (!key || !value) return -1;
    if (kvs_large_exist(key) == 0) {
        //printf("[LARGE_SET] key=%s exist\n", key);
        return 1;
    }    
    FILE *data_fp = fopen(VALUE_DATA_FILE, "ab+");
    if (!data_fp) return -1;

    // 计算当前偏移
    fseek(data_fp, 0, SEEK_END);
    long offset = ftell(data_fp);
    int len = strlen(value);

    // 写入数据
    fwrite(value, 1, len, data_fp);
    fflush(data_fp);
    fclose(data_fp);

    // 写入索引
    FILE *index_fp = fopen(VALUE_INDEX_FILE, "ab+");
    if (!index_fp) return -1;

    kvs_large_index_t idx = {0};
    strncpy(idx.key, key, MAX_KEY_LEN - 1);
    idx.offset = offset;
    idx.length = len;
    idx.deleted = 0;

    fwrite(&idx, sizeof(idx), 1, index_fp);
    fflush(index_fp);
    fclose(index_fp);

   // printf("[LARGE_SET] key=%s offset=%ld len=%d\n", key, offset, len);
    return 0;
}

// ======================
// 读取大 value（取最后有效记录）
// ======================
char *kvs_large_get(char *key) {
    if (!key) return NULL;

    FILE *index_fp = fopen(VALUE_INDEX_FILE, "rb");
    if (!index_fp) return NULL;

    kvs_large_index_t idx, latest = {0};
    int found = 0;
    //printf("search key:%s\n",key);
    while (fread(&idx, sizeof(idx), 1, index_fp)) {
        if (strcmp(idx.key, key) == 0) {
            if (idx.deleted) {
                found = 0; // 已删除
                //printf("not found\n");
            } else {
                latest = idx;
                found = 1;
                //printf("found\n");
            }
        }
    }
    fclose(index_fp);

    if (!found) return NULL;

    FILE *data_fp = fopen(VALUE_DATA_FILE, "rb");
    if (!data_fp) return NULL;

    fseek(data_fp, latest.offset, SEEK_SET);
    char *buf = kvs_malloc(latest.length + 1);
    fread(buf, 1, latest.length, data_fp);
    buf[latest.length] = '\0';
    fclose(data_fp);

    return buf;
}

// ======================
// 删除（写墓碑）
// ======================
int kvs_large_del(char *key) {
    if (!key) return -1;

    FILE *index_fp = fopen(VALUE_INDEX_FILE, "ab+");
    if (!index_fp) return -1;

    kvs_large_index_t idx = {0};
    strncpy(idx.key, key, MAX_KEY_LEN - 1);
    idx.deleted = 1;

    fwrite(&idx, sizeof(idx), 1, index_fp);
    fflush(index_fp);
    fclose(index_fp);

    //printf("[LARGE_DEL] key=%s tombstone written\n", key);
    return 0;
}

// ======================
// 修改（即追加新版本）
// ======================
int kvs_large_mod(char *key, char *value) {
    if (!key || !value) return -1;

    // 检查 key 是否存在
    if (kvs_large_exist(key) != 0) {
        printf("[LARGE_MOD] key=%s not exist\n", key);
        return -1;
    }

    // === 1. 写 tombstone 标记旧版本删除 ===
    FILE *index_fp = fopen(VALUE_INDEX_FILE, "ab+");
    if (!index_fp) return -1;

    kvs_large_index_t tomb = {0};
    strncpy(tomb.key, key, MAX_KEY_LEN - 1);
    tomb.deleted = 1;

    fwrite(&tomb, sizeof(tomb), 1, index_fp);
    fflush(index_fp);
    fclose(index_fp);

    // === 2. 写入新 value（与 set 类似） ===
    FILE *data_fp = fopen(VALUE_DATA_FILE, "ab+");
    if (!data_fp) return -1;

    fseek(data_fp, 0, SEEK_END);
    long offset = ftell(data_fp);
    int len = strlen(value);

    fwrite(value, 1, len, data_fp);
    fflush(data_fp);
    fclose(data_fp);

    // === 3. 写入新索引 ===
    index_fp = fopen(VALUE_INDEX_FILE, "ab+");
    if (!index_fp) return -1;

    kvs_large_index_t new_idx = {0};
    strncpy(new_idx.key, key, MAX_KEY_LEN - 1);
    new_idx.offset = offset;
    new_idx.length = len;
    new_idx.deleted = 0;

    fwrite(&new_idx, sizeof(new_idx), 1, index_fp);
    fflush(index_fp);
    fclose(index_fp);

    //printf("[LARGE_MOD] key=%s modified, new offset=%ld len=%d\n", key, offset, len);
    return 0;
}

// ======================
// 判断是否存在（扫描 index 最后状态）
// ======================
int kvs_large_exist(char *key) {
    if (!key) return -1;

    FILE *index_fp = fopen(VALUE_INDEX_FILE, "rb");
    if (!index_fp) return -1;

    kvs_large_index_t idx;
    int exist = -1;

    while (fread(&idx, sizeof(idx), 1, index_fp)) {
        if (strcmp(idx.key, key) == 0) {
            exist = idx.deleted ? -1 : 0;
        }
    }

    fclose(index_fp);
    return exist;
}