# coding:utf-8
from pyspark import SparkContext,SparkConf
from time import time
start = time()

if __name__=='__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
    # 通过conf 构建 SparkContext
    sc = SparkContext(conf=conf)

    # 读文件
    rdd1 = sc.textFile("../resources/words.txt")
    # 分割单词
    rdd2 = rdd1.flatMap(lambda line:line.split(" "))
    # 单词转化为元组
    rdd3 = rdd2.map(lambda x: (x,1))
    # 将字母按照key分组，将value 累加
    rdd4 = rdd3.reduceByKey(lambda a, b: a+b)
    # 读取 result
    print(rdd4.collect())

    # 这里rdd3已经被删除了，因此会重新执行rdd1到rdd3 在计算rdd5 会浪费计算资源，所以需要把rdd3保留
    rdd5 = rdd3.map(lambda x: x[1]+1)
    rdd6 = rdd5.filter(lambda x:x>1)
    print(rdd6.collect())
print(time()-start)
# 12.597682476043701





