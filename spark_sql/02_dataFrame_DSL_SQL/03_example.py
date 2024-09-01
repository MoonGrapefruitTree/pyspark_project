# encoding:utf-8

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ =="__main__":
    # sql入口
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # rdd入口
    sc = spark.sparkContext

    rdd = sc.textFile("../resources/words.txt").flatMap(lambda x:x.split(" ")).map(lambda x:[x])

    df = spark.createDataFrame(rdd,schema=["word"])

    # SQL
    df.createTempView("words")

    spark.sql("select word,count(*) as cnt from words group by word order by cnt desc").show()

    # DSL
    df1 = spark.read.format("text").load("../resources/words.txt")
    # withColumn 对每一列进行操作返回新列，名字相同则替换，否则新增
    df2 = df1.withColumn("value", F.explode(F.split(df1["value"], " ")))

    #
    df2.groupBy("value").count().\
        withColumnRenamed("value","word").\
        withColumnRenamed("count","cnt").\
        orderBy("cnt",ascending=False).show()

