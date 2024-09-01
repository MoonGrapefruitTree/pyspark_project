# coding:utf-8
from pyspark import SparkContext,SparkConf
conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__=="__main__":
    # 通过外部文件夹创建RDD读取文件夹下的所有小文件
    rdd = sc.textFile("../resources/tiny_files")
    print(rdd.glom().collect())  # 查看内容
