# coding:utf-8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
# 通过conf 构建 SparkContext
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    rdd1 = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('c', 1)])
    # groupByKey 按照key 进行分组  返回rdd
    result = rdd1.groupByKey()
    print(result.collect())
    # [('b', <pyspark.resultiterable.ResultIterable object at 0x000002C639757430>),
    # ('a', <pyspark.resultiterable.ResultIterable object at 0x000002C639757BB0>),
    # ('c', <pyspark.resultiterable.ResultIterable object at 0x000002C639757C10>)]
    # 和reduceByKey 不同的是这里没有聚合(即统计信息) 逻辑只是分组了
    result1 = result.map(lambda x:(x[0],list(x[1])))
    print(result1.collect())
    # [('b', [1]), ('a', [1, 1]), ('c', [1])]