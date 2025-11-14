#include "kvstore.h"
int g_network_mode = NETWORK_REACTOR;
int g_enable_rdb = 1;
int g_enable_aof = 1;
int ENABLE_RDB=1;
int ENABLE_AOF=1;
static void trim(char *str)
{
    // 去掉前后空白
    char *p = str;
    while (isspace((unsigned char)*p)) p++;
    memmove(str, p, strlen(p) + 1);

    // 去掉尾部空白
    char *end = str + strlen(str) - 1;
    while (end >= str && isspace((unsigned char)*end)) {
        *end = '\0';
        end--;
    }
}

int load_config(){
    FILE *fp = fopen("kvstore.conf", "r");
    if (!fp) {
        perror("open app.conf");
        return -1;
    }

    char line[256];

    while (fgets(line, sizeof(line), fp)) {

        trim(line);

        // 跳过空行和注释
        if (line[0] == '\0' || line[0] == '#')
            continue;

        char key[128] = {0};
        char value[128] = {0};

        // 通用格式: key = value 或 key=value
        if (sscanf(line, "%127[^=]=%127s", key, value) == 2) {

            trim(key);
            trim(value);

            // network_mode
            if (strcmp(key, "network_mode") == 0) {
                if (strcmp(value, "reactor") == 0)
                    g_network_mode = NETWORK_REACTOR;
                else if (strcmp(value, "proactor") == 0)
                    g_network_mode = NETWORK_PROACTOR;
                else if (strcmp(value, "ntyco") == 0)
                    g_network_mode = NETWORK_NTYCO;
                continue;
            }

            // ENABLE_RDB
            if (strcmp(key, "ENABLE_RDB") == 0) {
                ENABLE_RDB = atoi(value);
                if(ENABLE_RDB)printf("ENABLE_RDB\n");
                continue;
            }

            // ENABLE_AOF
            if (strcmp(key, "ENABLE_AOF") == 0) {
                ENABLE_AOF = atoi(value);
                if(ENABLE_AOF)printf("ENABLE_AOF\n");
                continue;
            }
        }
    }
    
    fclose(fp);
    return 0;
}