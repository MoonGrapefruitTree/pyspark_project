# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd = sc.parallelize([('a',1),('a',1),('a',1),('b',1),('b',1),('c',1)])

    # filter 通过函数对数据进行过滤 返回rdd
    # 传入函数，范围值为bool ture 会保留，否则丢弃
    result = rdd.filter(lambda t: t[0] == "a")
    print(result.collect())
    # [('a', 1), ('a', 1), ('a', 1)]
