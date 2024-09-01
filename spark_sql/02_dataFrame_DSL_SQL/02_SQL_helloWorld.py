# encoding:utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType
import pandas as pd

if __name__ =="__main__":
    # sql入口
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # rdd入口
    sc = spark.sparkContext



    # 读文件 csv参数设置多  schema 不设置的话默认都是string
    df = spark.read.format("csv").\
        option("sep",",").\
        option("header",True).\
        option("encoding","utf-8").\
        schema("id INT, class STRING, score INT").\
        load("../resources/stu_score.csv")

    # 首先要注册临时表
    df.createTempView("score1") # 注册临时表
    df.createOrReplaceTempView("score2") # 注册临时表，如果存在则替换
    df.createGlobalTempView("score3") # 创建全局表，可以在不同的session中使用

    spark.sql("select id,class,score from score1 where id = 2").show()
    spark.sql("select id,class,score from score2 where id = 2").show()
    # 这里需要前缀
    spark.sql("select id,class,score from global_temp.score3 where id = 2").show()

