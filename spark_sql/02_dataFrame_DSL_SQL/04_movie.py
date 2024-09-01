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


    schema = StructType().add("user_id",StringType(),nullable=False).\
        add("movie_id",StringType(),nullable=False).\
        add("rank",IntegerType(),nullable=False).\
        add("ts",StringType(),nullable=True)

    # 读取数据
    df = spark.read.format("csv").\
        option("sep","\t").\
        option("header",False).\
        option("encoding","utf-8").\
        schema(schema=schema).load("../resources/u_data.txt")

    df.show(5)

    # TODO 1  用户平均分
    df.groupBy("user_id").\
        avg("rank").\
        withColumnRenamed("avg(rank)","avg_rank").\
        withColumn("avg_rank",F.round("avg_rank",2)).\
        orderBy("avg_rank",ascending=False).\
        show(10)

    # TODO 2  电影的平均分

    df.groupBy("movie_id"). \
        avg("rank"). \
        withColumnRenamed("avg(rank)", "avg_rank"). \
        withColumn("avg_rank", F.round("avg_rank", 2)). \
        orderBy("avg_rank", ascending=False). \
        show(10)

    # TODO 3  查询大于平均分电影的数量
    print(df.where(df["rank"]>df.select(F.avg(df["rank"])).first()['avg(rank)']).\
        count())
    # TODO 4  高分电影中打分最多的人的平均分
    user_id = df.where(df["rank"]>3).\
        groupBy("user_id").\
        count().\
        orderBy("count",ascending= False).\
        limit(1).\
        first()["user_id"]
    df.filter(df["user_id"] ==user_id ).select(F.round(F.avg(df["rank"]),2)).show()

    # TODO 5 查询每个用户的平均，最低和最高打分
    df.groupBy("user_id").\
        agg(
        F.round(F.avg(df["rank"]),2).alias("avg_rank"),
        F.max("rank").alias("max_rank"),
        F.min("rank").alias("min_rank")
    ).show()

    # TODO 6 评分超过100次的电影的平均份top10
    df.groupBy("movie_id"). \
        agg(
        F.round(F.avg(df["rank"]), 2).alias("avg_rank"),
        F.count("movie_id").alias("cnt"),
    ).filter("cnt>100").orderBy("avg_rank",ascending=False).limit(10).show()