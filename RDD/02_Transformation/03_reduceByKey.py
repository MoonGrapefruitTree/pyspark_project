# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd = sc.parallelize([('a',1),('a',1),('a',1),('b',1),('b',1),('c',1)])

    # reduceByKey 针对k-v数据按照key分组，按照聚合逻辑进行聚合，返回对应rdd
    # 这里a，b表示同一个key的两个值
    print(rdd.reduceByKey(lambda a,b:a+b).collect())
    # [('b', 2), ('a', 3), ('c', 1)]