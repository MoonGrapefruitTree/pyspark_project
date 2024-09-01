# coding:utf-8
from pyspark import SparkContext,SparkConf
conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__=="__main__":
    # 通过并行集合创建RDD
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9])
    print(rdd.getNumPartitions())#查看默认分区  和CPU有关
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],5)
    print(rdd.getNumPartitions())#查看设置分区

    print(rdd.collect())#分布式数据RDD转为本地数据
