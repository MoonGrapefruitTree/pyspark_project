# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8],3)
    # mapPartitions 分区处理数据 一次处理一个分区，而不是一次处理一个数据
    result = rdd1.mapPartitions(lambda iter: [i*10 for i in iter])

    print(result.collect())
    # [10, 20, 30, 40, 50, 60, 70, 80]