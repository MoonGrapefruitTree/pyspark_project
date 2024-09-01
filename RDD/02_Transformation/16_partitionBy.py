# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([("a",1),("b",3),("a",2),("a",5),("a",4)],3)
    # partitionBy 分区处理数据 一次处理一个分区，而不是一次处理一个数据
    print(rdd1.glom().collect())
    # [[('a', 1)], [('b', 3), ('a', 2)], [('a', 5), ('a', 4)]]
    # 这里x是key值
    result = rdd1.partitionBy(2,lambda x: 1 if x =="a" else 2)
    print(result.glom().collect())
    # [[('b', 3)], [('a', 1), ('a', 2), ('a', 5), ('a', 4)]]