# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([('b', 1), ('a', 1), ('b', 1), ('c', 1)])
    # collect 获得所有分区，返回list  返回list
    result = rdd1.collect()
    print(result)
    # defaultdict(<class 'int'>, {'b': 2, 'a': 1, 'c': 1})