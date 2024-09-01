# coding:utf-8
from pyspark import SparkContext,SparkConf
conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__=="__main__":
    # 通过外部文件创建RDD
    rdd = sc.textFile("../resources/words.txt")
    print(rdd.getNumPartitions())  # 查看默认分区 和文件有关，如果是hdfs则和block有关
    print(rdd.collect())  # 查看内容

    # 查看参数作用
    rdd2 = sc.textFile("../resources/words.txt",3)
    rdd3 = sc.textFile("../resources/words.txt",100)
    print(rdd2.getNumPartitions())  # 查看分区 最小值为参考
    print(rdd3.getNumPartitions())  # 查看分区 不能太离谱


    # rdd4=sc.wholeTextFiles()
    # 读取小文件专用，提高性能。