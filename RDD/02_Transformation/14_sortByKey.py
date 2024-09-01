# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([('b', 1), ('a', 1), ('b', 1), ('c', 1)])
    # sortByKey 按照key排序  返回rdd
    result = rdd1.sortByKey(True,keyfunc=lambda x:x.lower())
    print(result.collect())
    # [('a', 1), ('b', 1), ('b', 1), ('c', 1)]