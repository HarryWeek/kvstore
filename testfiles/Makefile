# 编译器
CC = gcc

# 编译选项
CFLAGS = -Wall -O2

# 自动获取所有 C 文件
SRCS = $(wildcard *.c)

# 把 .c 换成可执行文件名（去掉扩展名）
TARGETS = $(SRCS:.c=)

# 默认目标：编译所有程序
all: $(TARGETS)

# 规则：每个 .c 生成同名可执行文件
%: %.c
	$(CC) $(CFLAGS) -o $@ $<

# 清理
clean:
	rm -f $(TARGETS)

.PHONY: all clean
