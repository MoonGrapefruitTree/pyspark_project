# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([1,2,3,4,5,6,7,8,9],2)
    # glom 将数据按分区加嵌套  返回rdd
    result = rdd1.glom()
    print(result.collect())
    # [[1, 2, 3, 4], [5, 6, 7, 8, 9]]
    result1 = rdd1.glom().flatMap(lambda x:x)
    print(result1.collect())
    # [1, 2, 3, 4, 5, 6, 7, 8, 9]
