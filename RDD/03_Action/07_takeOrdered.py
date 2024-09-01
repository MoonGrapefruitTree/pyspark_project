# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8]*3)
    # takeOrdered 排序后取前几个，按照什么排序可以自定义
    result1 = rdd1.takeOrdered(5,lambda x:abs(x-5))
    print(result1)
    # [5, 5, 5, 4, 6]





