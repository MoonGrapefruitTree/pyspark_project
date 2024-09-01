# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('c', 1)])
    rdd2 = sc.parallelize([('a', 1), ('b', 1)])
    # union 合并两个rdd 返回rdd
    result = rdd1.union(rdd2)
    print(result.collect())
    # [('a', 1), ('a', 1), ('b', 1), ('c', 1), ('a', 1), ('b', 1)]
    # 默认不去重，且不同类型也能合并


