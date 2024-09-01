# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8]*3)
    # takeSample 随机取几个数据 参数1 bool true 允许相同，false 不允许相同 参数2 取几个，参数三随机种子
    result1 = rdd1.takeSample(False,5,2)
    print(result1)
    # 随机取5个 位置不重复，数可以重复

    result2 = rdd1.takeSample(False, 9, 2)
    print(result2)
    # 1



