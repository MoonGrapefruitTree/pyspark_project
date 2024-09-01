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
    rdd = sc.parallelize([1,2,3,4,5],3).map(lambda x:[x])
    df = rdd.toDF(['num'])
    # 使用rdd的mapPartitions 进行操作 ，分区必须只有一个
    single_rdd = df.rdd.repartition(1)

    def process(iter):
        sum=0
        for row in iter:
            sum += row['num']
        return [sum]

    print(single_rdd.mapPartitions(process).collect())