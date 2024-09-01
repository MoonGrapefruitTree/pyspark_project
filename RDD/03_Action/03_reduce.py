# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([1,2,3,4,5,6,7,8])
    # reduce  对rdd进行聚合，返回聚合结果  返回结果的类型
    result = rdd1.reduce(lambda a,b:a+b)
    print(result)
    # 36
    result = rdd1.reduce(lambda a, b: a + b if a % 2 == 0 and b % 2 == 0 else 0)
    print(result)
    # 8 这里是int 这里可以自己算下，reduce 就是两个计算结果后将结果和下一个计算。