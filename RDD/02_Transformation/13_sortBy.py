# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([9,5,4,7,8,6,2,])
    # sortBy 按照自定义函数对每个分区进行排序，全局时分区为1 True 升序 Flase 降序 这里第三个参数为分区数， 返回rdd
    result = rdd1.sortBy(lambda x:x,True)
    print(result.collect())
    # [2, 4, 5, 6, 7, 8, 9]
    result1 = rdd1.sortBy(lambda x:x,False)
    print(result1.collect())
    # [9, 8, 7, 6, 5, 4, 2]