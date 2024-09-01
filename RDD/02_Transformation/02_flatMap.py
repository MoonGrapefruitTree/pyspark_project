# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd = sc.parallelize([[1, 2, 3], [4, 5, 6], [7, 8], [9]])

    # flatMap 先处理map逻辑再解除嵌套
    print(rdd.map(lambda x: [i*10 for i in x]).collect())
    # [[10, 20, 30], [40, 50, 60], [70, 80], [90]]
    print(rdd.flatMap(lambda x: [i*10 for i in x]).collect())
    # [10, 20, 30, 40, 50, 60, 70, 80, 90]