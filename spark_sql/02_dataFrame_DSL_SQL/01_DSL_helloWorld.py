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

    # 获得列
    id_col = df['id']
    class_col  =df["class"]
    # 不同的选择方法
    df.select(["id","class"]).show()
    df.select("id", "class").show()
    df.select([id_col,class_col]).show()
    # 过滤
    df.filter("id=2").show()
    df.filter(df['id']==2).show()
    df.where("id=2").show()
    df.where(df['id']==2).show()

    # 分组统计
    df.groupBy("class").count().show()
    df.groupBy(df['class']).count().show()

    # groupBy返回值不是dataframe
    r = df.groupBy("class")

    print(type(df),type(r))