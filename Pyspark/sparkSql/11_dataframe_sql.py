# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

if __name__ == '__main__':

    # 相当于 sparkconf
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    # sparkContext 对象
    sc = spark.sparkContext

    #读取文件类型 / 指定列名 / 加载文件
    df = spark.read.format("csv").\
        schema("name STRING, age INT, sc STRING").\
        load("../input/people.csv")

    #注冊成临时表
    df.createTempView("score") #注册临时视图
    df.createOrReplaceTempView("score2") #注册或替换临时视图
    df.createGlobalTempView("score3") #注册全局临时视图，使用时需要带上global_temp. 前缀

    #通过sparksession对象的 sql API语句执行
    spark.sql("select * from score").show()
    spark.sql("select * from score2").show()
    spark.sql("select * from global_temp.score3").show()