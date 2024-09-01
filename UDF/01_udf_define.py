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

    rdd = sc.parallelize([1,2,3,4,5,6,7]).map(lambda x: [x])
    df = rdd.toDF(["num"])
    # TODO 定义一个，可以用sql和DSL来使用这个UDF
    # 自定义函数
    def num_ride_10(num):
        return num*10
    # 注册自定义函数
    # 自定义名称， 方法名称， 返回值类型
    udf2 = spark.udf.register("udf1",num_ride_10,IntegerType())

    # 使用以下连两个方法方法掉用
    # 这里num为列明
    df.selectExpr("udf1(num)").show()
    # 这里传入类的方式实现
    df.select(udf2(df["num"])).show()

    # TODO 另一种注册UDF方式，只能用DSL调用
    # 这里没名字，因此只能使用DSL
    udf3 = F.udf(num_ride_10,IntegerType())
    df.select(udf3(df["num"])).show()