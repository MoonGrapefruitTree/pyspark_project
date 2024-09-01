# encoding:utf-8

from pyspark.sql import SparkSession

if __name__ =="__main__":
    # sql入口
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # rdd入口
    sc = spark.sparkContext

    df = spark.read.csv("resources/stu_score.txt",sep=',', header = False)
    df2=df.toDF("id","class","score")
    df2.printSchema()
    df2.show()


    df2.createTempView("score")

    # 以下是两种数据处理的方式
    # SQL
    spark.sql("""
    select * from score where class = '语文' limit 1
    """).show()
    # DSL
    df2.where("class='语文'").limit(1).show()