# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([("a",1),("b",3),("a",2),("a",5),("a",4)],3)
    # repartition 分区处理数据 一次处理一个分区，而不是一次处理一个数据  分区除了全局排序一般不进行自己加分区
    # 这个最好不用
    print(rdd1.glom().collect())
    # [[('a', 1)], [('b', 3), ('a', 2)], [('a', 5), ('a', 4)]]
    # 这里x是key值
    result = rdd1.repartition(2)
    print(result.glom().collect())
    # [[('a', 1), ('a', 5), ('a', 4)], [('b', 3), ('a', 2)]]

    # coalesce 和上面一样但是在增加分区时需要额外参数
    result2 = rdd1.coalesce(4,shuffle=True)
    print(result2.glom().collect())
