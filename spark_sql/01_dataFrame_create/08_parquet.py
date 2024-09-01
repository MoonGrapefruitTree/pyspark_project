# encoding:utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType
import pandas as pd

if __name__ =="__main__":
    # sql入口
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # rdd入口
    sc = spark.sparkContext



    # 读文件 parquet 没有这个类型的数据样本，但是可以直接用，不用额外设置
    df = spark.read.format("parquet").load("../resources/stu_score.csv")
    df.printSchema()
    df.show()