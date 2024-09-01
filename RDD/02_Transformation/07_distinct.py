# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd = sc.parallelize([('a',1),('a',1),('a',1),('b',1),('b',1),('c',1)])

    # distinct 通过函数对数据进行分组 返回rdd
    result = rdd.distinct()
    print(result.collect())
    # [('b', 1), ('c', 1), ('a', 1)] 这里并不按顺序返回，返回顺序不固定


