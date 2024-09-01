# 广播变量  # 问题提出：分区执行本地数据和分布式数据join时，会需要将本地数据通过网络给各个分布式机器。这样会导致数据复制很多份导致空间浪费。
from pyspark import SparkContext,SparkConf
conf = SparkConf().setMaster("local[*]").setAppName("testApp")
sc = SparkContext(conf=conf)



if __name__ =="__main__":
    # 这个会在每个分布式进程使用，因此需要声明为广播变量
    stu_inf_list=[(1,"张三",17),(2,"张四",54),(3,"张二",15),(4,"张一",12)]
    broadcast = sc.broadcast(stu_inf_list)




    score_info_rdd = sc.parallelize([(1,"语文",99),(1,"数学",99),(1,"英语",99),
                                 (2,"语文",99),(2,"数学",99),(2,"英语",99),
                                 (3,"语文",99),(3,"数学",99),(3,"英语",99),
                                 (4,"语文",99),(4,"数学",99),(4,"英语",99),
                                 (5,"语文",99),(5,"数学",99),(5,"英语",99)])




    def map_func(data):
        for stu in broadcast.value:
            if data[0]==stu[0]:
                return (data[0],stu[1],data[1],data[2])
            else:
                continue
        return (data[0],None,data[1],data[2])

    result_rdd = score_info_rdd.map(map_func)
    print(result_rdd.collect())
