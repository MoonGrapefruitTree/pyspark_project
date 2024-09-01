# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8])
    # foreach 和map类似对每个元素进行操作 但是无返回值 无返回值 并行输出，效率高，可以用于往数据库写数据。
    rdd1.foreach(lambda x:print(abs(x-5)))
    print(rdd1.collect())
    # 无须
    # 2
    # 1
    # 3
    # 3
    # 2
    # 1
    # 4
    # 0
    # [1, 2, 3, 4, 5, 6, 7, 8]





