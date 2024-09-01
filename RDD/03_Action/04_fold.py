# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([1,2,3,4,5,6,7,8],3)
    # fold 带初值的聚合，reduce的增强，分区内聚合有，分区间也有  返回结果的类型
    result = rdd1.fold(10,lambda a,b:a+b)
    print(result)
    # 76 这里sum(1-8）=36 但是三个分区，每一个分区加一个10 分区间加一个10 一共4个10 因此返回40+36=76