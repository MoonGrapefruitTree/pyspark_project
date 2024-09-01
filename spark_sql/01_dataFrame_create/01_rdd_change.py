# encoding:utf-8

from pyspark.sql import SparkSession

if __name__ =="__main__":
    # sql入口
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # rdd入口
    sc = spark.sparkContext

    # 构建列表形式rdd
    rdd = sc.textFile("../resources/stu_score.txt").map(lambda x:x.split(',')).map(lambda x:(int(x[0]),x[1],int(x[2])))
    # 从rdd构建df
    df = spark.createDataFrame(rdd,schema=['id','class','score'])
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
    select * from score where id<2
    """).show()