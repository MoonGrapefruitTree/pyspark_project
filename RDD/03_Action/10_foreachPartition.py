# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8], 3)
    # foreachPartition 分区处理数据 一次处理一个分区，而不是一次处理一个数据 但是无返回值
    def process(iter):
        for i in iter:
            print(i)

    result = rdd1.foreachPartition(process)

    print(result)
    # [10, 20, 30, 40, 50, 60, 70, 80]