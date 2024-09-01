# coding:utf-8
from pyspark import SparkContext,SparkConf
conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__=="__main__":

    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9])


    def add(x):
        return x*10
    # map 一条条处理rdd中的数据 返回新的rdd
    print(rdd.map(lambda x: x * 10).collect())
    print(rdd.map(add).collect())