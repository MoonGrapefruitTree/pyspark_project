# 广播变量  # 问题提出：分区执行本地数据和分布式数据join时，会需要将本地数据通过网络给各个分布式机器。这样会导致数据复制很多份导致空间浪费。
# 标记为广播变量后，可以减少发数据的次数。
# 累加器
# 综合案例