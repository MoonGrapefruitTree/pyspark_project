# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([('b', 1), ('a', 1), ('b', 1), ('c', 1)])
    # countByKey 计算同一个key的数量  返回dict
    result = rdd1.countByKey()
    print(result)
    # defaultdict(<class 'int'>, {'b': 2, 'a': 1, 'c': 1})