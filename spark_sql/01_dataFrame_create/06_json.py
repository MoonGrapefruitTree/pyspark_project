# encoding:utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType
import pandas as pd

if __name__ =="__main__":
    # sql入口
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # rdd入口
    sc = spark.sparkContext



    # 读文件 和txt的不同之处在于json文件几乎不用设置列名，因为json自带
    df = spark.read.format("json").load("../resources/people.json")
    df.printSchema()
    df.show()