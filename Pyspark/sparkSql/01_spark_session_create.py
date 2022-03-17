# -*- coding: utf-8 -*-


'''Sparksession 对象的导入, 对象来自于spark.sql包中'''
from pyspark.sql import SparkSession

if __name__ == '__main__':

    '''执行SparkSession执行环境入口对象'''
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()

    #通过sparkSession对象获取Sparkcontext对象
    sc = spark.sparkContext

    #sparksql的 helloworld
    df = spark.read.csv("../input/worlds.txt", sep=" ", header=False)
    df1 = df.toDF("a", "b", "c")
    df1.printSchema()
    df1.show()

    df1.createTempView("score")

    #sql分割
    spark.sql("select * from score limit 1").show()

    #DSL分割
    df1.limit(1).show()