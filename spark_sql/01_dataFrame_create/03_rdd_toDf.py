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
    # 从rdd构建df简单范式
    # df = rdd.toDF(['id','class','score'])
    # 细节模式
    schame = StructType().add("id",IntegerType(),nullable=False).add("class", StringType(), nullable=True).add("score", IntegerType(), nullable=True)
    df = rdd.toDF(schema=schame)
    # 打印表结构
    df.printSchema()

    # 打印数据
    # 5表示展示5条
    # False 表示不截断，全部展示
    df.show(5,False)


    # 创建临时表使用sql查询
    df.createTempView("score")
    # sql 查询
    spark.sql("""
    select * from score where id=3
    """).show()