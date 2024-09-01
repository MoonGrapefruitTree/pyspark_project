# encoding:utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType
import pandas as pd

if __name__ =="__main__":
    # sql入口
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # rdd入口
    sc = spark.sparkContext


    # 构建StructType 这里text一行数据为数据表的一个数据，默认类型为String
    structType = StructType().add("data", StringType(), nullable=True)
    # 读文件
    df = spark.read.format("text").schema(schema=structType).load("../resources/stu_score.txt")
    df.printSchema()
    df.show()