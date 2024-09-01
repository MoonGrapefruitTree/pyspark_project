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


    schema = StructType().add("id",StringType(),nullable=True).\
        add("class",StringType(),nullable=True).\
        add("score",IntegerType(),nullable=True)

    # 读取数据
    df = spark.read.format("csv").\
        option("sep",",").\
        option("header",False).\
        option("encoding","utf-8").\
        schema(schema=schema).load("../resources/stu.csv")

    df.show()
    # 去除完全重复的值
    df.dropDuplticates().show()
    # 某些属性重复的值
    df.dropDuplicaes(["id","class"]).show()

    # 去除空值
    df.dropna().show()
    # thresh 两个有效列才能留下否则删除
    df.dropna(thresh=2).show()

    # 在这个集合里满足thresh
    df.dropna(thresh=2,subset=["id","class"]).show()

    # fillna 填充
    df.fillna("loss").show()

    # fillna 填充对应列填充
    df.fillna("loss",subset=["id","class"]).show()

    # fillna 字典填充
    df.fillna({"id":"noid","class":"noclass","score":"0"}).show()