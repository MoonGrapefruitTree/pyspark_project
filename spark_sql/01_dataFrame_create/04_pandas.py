# encoding:utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType
import pandas as pd

if __name__ =="__main__":
    # sql入口
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # rdd入口
    sc = spark.sparkContext

    # 使用pandas 构建df
    # pdf = pd.read_csv("../resources/stu_score.txt",encoding="utf-8")
    pdf = pd.DataFrame({
        "id":[1,2,3],
        "name":["张三","李四","王五"],
        "score":[60,59,58]
    })
    print(pdf)
    # 从pandas 的df 构建df简单方式
    df = spark.createDataFrame(pdf)
    # # 详细构建模式
    # schame = StructType().add("id",IntegerType(),nullable=False).add("class", StringType(), nullable=True).add("score", IntegerType(), nullable=True)
    # df = rdd.toDF(schema=schame)
    # 打印表结构
    df.printSchema()
    df.show(5)

    # # 创建临时表使用sql查询
    # df.createTempView("score")
    # # sql 查询
    # spark.sql("""
    # select * from score where id=3
    # """).show()