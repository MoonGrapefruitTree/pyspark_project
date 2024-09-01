# utf-8
# 广播变量  # 问题提出：分区执行本地数据和分布式数据join时，会需要将本地数据通过网络给各个分布式机器。这样会导致数据复制很多份导致空间浪费。
from pyspark import SparkContext,SparkConf
conf = SparkConf().setMaster("local[*]").setAppName("testApp")
sc = SparkContext(conf=conf)



if __name__ =="__main__":
    # 这段代码的问题在于 num被传到分区后，给了num的值而不是地址，因此在每个分区内num增加为每个分区的数量，并且最终print时num并没有变化还是0
    # 使用accumulator 创建累加器 则能正常累加 rdd和第二次计算时累加器会有变化，因此需要在执行动作算子之前进行缓存
    rdd = sc.textFile("../resources/words.txt")

    rdd1 = rdd.map(lambda x:x.split(" "))
    char_set=[".",":","!","?","#"]
    broadcast = sc.broadcast(char_set)
    num_char = sc.accumulator(0)
    num_word = sc.accumulator(0)
    def map_funx(data):
        global num_char,num_word
        for x in data:
            if x in broadcast.value:
                num_char += 1
            else:
                num_word += 1
        return data
    print(rdd1.map(map_funx).collect())

    print(num_char)
    print(num_word)