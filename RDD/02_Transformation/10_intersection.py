# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('c', 1)])
    rdd2 = sc.parallelize([('a', 1), ('b', 2)])
    # intersection 求交集  返回rdd
    result = rdd1.intersection(rdd2)
    print(result.collect())
    # [('a', 1)]
