# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8],1)
    # saveAsTextFile 保存数据
    rdd1.saveAsTextFile("../resources/output1")

# 文件数量和分区数一样