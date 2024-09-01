# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd = sc.parallelize([('a',1),('a',1),('a',1),('b',1),('b',1),('c',1)])

    # groupBy 通过函数对数据进行分组 返回rdd
    # 这里 lambda t:t[0] 指的是 按照key分组 这里函数可以自定义
    result = rdd.groupBy(lambda t: t[0])
    print(result.collect())
    # [('b', <pyspark.resultiterable.ResultIterable object at 0x000001987FEB6D60>),
    # ('a', <pyspark.resultiterable.ResultIterable object at 0x000001987FEB6DF0>),
    # ('c', <pyspark.resultiterable.ResultIterable object at 0x000001987FEB6E50>)]

    print(result.map(lambda x:(x[0],list(x[1]))).collect())
    # [('b', [('b', 1), ('b', 1)]), ('a', [('a', 1), ('a', 1), ('a', 1)]), ('c', [('c', 1)])]