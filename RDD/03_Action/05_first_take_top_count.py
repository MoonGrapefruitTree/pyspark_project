# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([1,2,3,4,5,6,7,8],3)
    # first 取出第一个数
    result_first = rdd1.first()
    print(result_first)
    # 1

    # take 取出前几个数
    result_take = rdd1.take(5)
    print(result_take)
    # [1, 2, 3, 4, 5]

    # top 降序排列后取出前几个数
    result_top = rdd1.top(5)
    print(result_top)
    # [8, 7, 6, 5, 4]

    # count 当前rdd有几个数
    result_count = rdd1.count()
    print(result_count)
    # 8


