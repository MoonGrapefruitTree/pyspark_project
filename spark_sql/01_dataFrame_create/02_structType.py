# encoding:utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType


if __name__ =="__main__":
    # sql入口
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # rdd入口
    sc = spark.sparkContext

    # 构建列表形式rdd
    rdd = sc.textFile("../resources/stu_score.txt").map(lambda x:x.split(',')).map(lambda x:(int(x[0]),x[1],int(x[2])))
    # 使用StructType构建
    schame = StructType().add("id",IntegerType(),nullable=False).add("class", StringType(), nullable=True).add("score", IntegerType(), nullable=True)
    # 构建df
    df = spark.createDataFrame(rdd,schema=schame)
    df.printSchema()
    df.show()
