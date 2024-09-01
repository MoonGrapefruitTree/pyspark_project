# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('c', 1)])
    rdd2 = sc.parallelize([('a', 2), ('b', 2)])
    # join 只能用于k-v数据 按照key 进行关联  如果用value 可以先用map 处理  返回rdd
    result = rdd1.join(rdd2)
    print(result.collect())
    # [('b', (1, 2)), ('a', (1, 2)), ('a', (1, 2))]
    result1 = rdd1.leftOuterJoin(rdd2)
    print(result1.collect())
    # [('b', (1, 2)), ('a', (1, 2)), ('a', (1, 2)), ('c', (1, None))]




