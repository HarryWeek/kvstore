# 9.1 Kvstore


### 面试题
1. 为什么会实现kvstore，使用场景在哪里？
   为了实现高并发高吞吐量和低延迟的内存存储
   使用场景：电商购物、演唱会抢票、高铁抢票等高并发场景，游戏服务器等低延迟场景

2. reactor, ntyco, io_uring的三种网络模型的性能差异？
   reactor 适合高并发和value小的场景
   ntyco io_uring适合value大写入耗时长的场景

3. 多线程的kvstore该如何改进？
   
4. 私有协议如何设计会更加安全可靠？
   统一的命令格式、明确的数据边界防止粘包、每条消息都应能验证来源与完整性、用序列号 / 时间戳控制顺序、
5. 协议改进以后，对已有的代码有哪些改变？
   代码上多了命令解析和命令合成的步骤，
6. kv引擎实现了哪些？
   哈希表、红黑树、数组
7. 每个kv引擎的使用场景，以及性能差异？
   数组性能差O(N)，大数据量性能差O(N)，红黑树性能较好O(nlogn),哈希表需要改进哈希算法，最优可以做到O(1)，最差O(N)
8. 测试用例如何实现？并且保证代码覆盖率超过90%
9.  网络并发量如何？qps如何？
10. 能够跟哪些系统交互使用？
    数据库系统、缓存系统（网页、搜索系统（保存搜索结果


### 架构设计
![image](https://disk.0voice.com/p/py)
写入性能
ntyco
rbtree_1w 100000--> time_used: 12480 ms, qps: 8012
proactor
rbtree_1w 100000--> time_used: 11781 ms, qps: 8488
reactor
rbtree_1w 100000--> time_used: 12504 ms, qps: 7997
主从同步性能
PROACTOR
rbtree_1w 100000--> time_used: 11679 ms, qps: 8562
REACTROR
rbtree_1w 100000--> time_used: 11909 ms, qps: 8397
NTYCO
rbtree_1w 100000--> time_used: 12268 ms, qps: 8151

哈希表写入 ntyco
hash_1w 10000--> time_used: 6093 ms, qps: 1641
哈希表读取 ntyco
hash_1w 10000--> time_used: 8351 ms, qps: 1197

红黑树读取 ntyco
rbtree_1w 10000--> time_used: 3758 ms, qps: 2660

数组写入 ntyco
array_1w 10000--> time_used: 3598 ms, qps: 2779
数组读取 
array_1w 10000--> time_used: 3672 ms, qps: 2723

1000字符
reactor
大数据写入
large_value_1w 10000 --> time_used: 1963 ms, qps: 5094
large_value_1w 100000 --> time_used: 18345 ms, qps: 5451
读取
large_value_1w 10000 --> time_used: 4392 ms, qps: 2276

proactor
大数据写入
large_value_1w 10000 --> time_used: 2027 ms, qps: 4933
large_value_1w 100000 --> time_used: 18162 ms, qps: 5506
读取
large_value_1w 10000 --> time_used: 4390 ms, qps: 2277

ntyco
大写入
large_value_1w 100000 --> time_used: 18817 ms, qps: 5314

3000字符
ntyco
write:large_value_1w 100000 --> time_used: 19811 ms, qps: 5047
read:large_value_1w 100000 --> time_used: 311018 ms, qps: 321
proactor
write:large_value_1w 100000 --> time_used: 18510 ms, qps: 5402
read:large_value_1w 100000 --> time_used: 295646 ms, qps: 338
reactor
write:large_value_1w 100000 --> time_used: 18773 ms, qps: 5326
read:large_value_1w 100000 --> time_used: 298573 ms, qps: 334