# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd = sc.parallelize([('a',1),('a',1),('a',1),('b',1),('b',1),('c',1)])

    # mapValues 针对k-v数据的value进行map操作
    # 这里a表示同k-v中的v
    print(rdd.mapValues(lambda a:a*10).collect())
    # [('a', 10), ('a', 10), ('a', 10), ('b', 10), ('b', 10), ('c', 10)]
    # 也可以这样
    print(rdd.map(lambda x: (x[0],x[1] * 10)).collect())