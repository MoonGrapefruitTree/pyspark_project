# encoding:utf-8
import string

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
    df = spark.read.format("csv"). \
        option("sep", ","). \
        option("header", True). \
        option("encoding", "utf-8"). \
        schema("id INT, class STRING, score INT"). \
        load("resources/stu_score.csv")

    df.createTempView("stu")

    # 简单测试
    spark.sql("select *, AVG(score) over(partition by id) as AVG_SCORE from stu").show()

