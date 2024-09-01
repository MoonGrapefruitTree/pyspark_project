# encoding:utf-8
import string

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, IntegerType, StringType,ArrayType





if __name__ =="__main__":
    # sql入口
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions",2).\
        getOrCreate()
    # rdd入口
    sc = spark.sparkContext
    # 数据
    rdd = sc.parallelize([[1],[2],[3]])
    df = rdd.toDF(['num'])

    def process(data):
        return {'num':data,"letters":string.ascii_letters[data]}

    # 字典需要用 StructType
    udf1 = spark.udf.register("udf1",process,StructType().\
                              add("num",IntegerType(),nullable=True).\
                              add("letters",StringType(),nullable=True))

    df.selectExpr("udf1(num)").show()
    df.select(udf1(df['num'])).show()