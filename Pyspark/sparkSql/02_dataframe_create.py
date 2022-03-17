# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    # 基于RDD转换成Dataframe
    sc = spark.sparkContext
    rdd = sc.textFile("../input/worlds.txt").\
        map(lambda x : x.split(" ")).\
        map(lambda x : (x[0], int(x[1]), x[2]))

    #构建Dataframe对象
    df = spark.createDataFrame(rdd, schema=["a", "b", "c"])
    df.printSchema()
    df.show()

    #将df对象转换成临时视图表
    df.createOrReplaceTempView("people")
    spark.sql("select * from people where b < 3").show()