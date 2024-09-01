# encoding:utf-8

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, IntegerType, StringType,ArrayType





if __name__ =="__main__":
    # sql入口
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions",2).\
        getOrCreate()
    # rdd入口
    sc = spark.sparkContext
    # 数据
    rdd = sc.parallelize([["hadoop flink spark"],["hadoop flink java"]])
    df = rdd.toDF(["line"])

    # 字符切分
    def split(data):
        return data.split(" ")

    udf1 = spark.udf.register("udf0",split,ArrayType(StringType()))
    # DLS
    df.select(udf1(df['line'])).show(truncate=False)
    # SQL
    df.createTempView("lines")
    spark.sql("select udf0(line) from lines").show(truncate=False)


    udf2 = F.udf(split,ArrayType(StringType()))
    df.select(udf2(df['line'])).show(truncate=False)