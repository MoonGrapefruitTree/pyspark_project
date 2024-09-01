# encoding:utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType
import pandas as pd

if __name__ =="__main__":
    # sql入口
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # rdd入口
    sc = spark.sparkContext



    # 读文件 csv参数设置多
    df = spark.read.format("csv").\
        option("sep",",").\
        option("header",True).\
        option("encoding","utf-8").\
        schema("id INT, class STRING, score INT").\
        load("../resources/stu_score.csv")
    df.printSchema()
    df.show()