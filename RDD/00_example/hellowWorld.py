# coding:utf-8
from pyspark import SparkContext,SparkConf

if __name__=='__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("WordCountHellowWorld")
    # 通过conf 构建 SparkContext
    sc = SparkContext(conf=conf)

    # 读文件
    file_rdd = sc.textFile("../resources/words.txt")
    # 分割单词
    words_rdd = file_rdd.flatMap(lambda line:line.split(" "))
    # 单词转化为元组
    word_with_one_rdd = words_rdd.map(lambda x: (x,1))
    # 将字母按照key分组，将value 累加
    result_rdd = word_with_one_rdd.reduceByKey(lambda a, b: a+b)
    # 读取 result
    print(result_rdd.collect())






