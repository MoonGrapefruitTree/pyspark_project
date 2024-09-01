# encoding:utf-8

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, IntegerType, StringType





if __name__ =="__main__":
    # sql入口
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions",2).\
        getOrCreate()
    # rdd入口
    sc = spark.sparkContext

    # Spark.sql.shuffle.partitions 默认为200，集群模式较为合适，local则可以石之为2，4，10等

    schema = StructType().add("user_id", StringType(), nullable=False). \
        add("movie_id", StringType(), nullable=False). \
        add("rank", IntegerType(), nullable=False). \
        add("ts", StringType(), nullable=True)

    # 读取数据
    df = spark.read.format("csv"). \
        option("sep", "\t"). \
        option("header", False). \
        option("encoding", "utf-8"). \
        schema(schema=schema).load("../resources/u_data.txt")

    df.show()
    # 写出txt
    df.select(F.concat_ws("---","user_id","movie_id","rank","ts"))\
        .write.mode("overwrite").format("text").save("../resources/u_data1")

    # 写出csv
    df.write.mode("overwrite").format("csv").\
        option("sep",",").save("../resources/u_data2")

    # 写出json
    df.write.mode("overwrite").format("json").save("../resources/u_data3")

    # 写出 parquet 默认值
    df.write.mode("overwrite").format("parquet").save("../resources/u_data4")