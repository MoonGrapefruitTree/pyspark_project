from pyspark.sql import SparkSession

sparkOff = SparkSession.builder.\
    appName("SparkSQL OffLine").\
    config("spark.sql.shuffle.partitions", "3").\
    getOrCreate()

