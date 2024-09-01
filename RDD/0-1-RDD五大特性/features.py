# coding:utf-8
from pyspark import SparkContext,SparkConf
conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__=="__main__":
    # 1、分区储存 以下代码三个分区
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)
    print(rdd.glom().collect()) # [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

    # 2、rdd方法会作用到所用的分区上
    print(rdd.map(lambda x:x*10).glom().collect()) # [[10, 20, 30], [40, 50, 60], [70, 80, 90]]

    # 3、RDD之间是有依赖关系（血缘关系）的。
    rdd1 = sc.textFile("../resources/words.txt")
    rdd2 = rdd1.flatMap(lambda line: line.split(" "))
    rdd3 = rdd2.map(lambda x: (x, 1))
    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd4.collect())

    # 4、k-v的RDD可以有分区器 按照key值进行分区
    # 5、分区规划会靠近数据所在服务器






