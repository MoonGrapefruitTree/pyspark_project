# 测试pyspark是否正确安装的简单程序
from pyspark import SparkContext
from pyspark import SparkConf


# 创建Spark配置对象
conf = SparkConf().setAppName("TestSparkInstallation")


# 创建Spark上下文对象
sc = SparkContext(conf=conf)


# 打印出Spark版本
print("Spark version is: " + sc.version)


# 停止Spark上下文
sc.stop()


