# -*- coding: utf-8 -*-

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    sc = spark.sparkContext

    #TODO 1: SQL风格进行处理
    rdd = sc.textFile("../input/worlds.txt").\
        flatMap(lambda x : x.split(" ")).\
        map(lambda x : [x])

    #加入列名
    df = rdd.toDF(["word"])
    #注册为表格
    df.createTempView("words")
    spark.sql("select word,count(*) count from words group by word").show()

    #TODO 2: DSL风格
    df = spark.read.format("text").\
        schema("word STRING").\
        load("../input/worlds.txt")

    #withcolumn对象, 对已存在的列操作返回新的列
    df1 = df.withColumn("word", F.explode(F.split(df["word"], " ")))

    df2 = df1.groupBy("word").\
        count().\
        withColumnRenamed("word","w1").\
        withColumnRenamed("count","cnt").\
        orderBy("word", ascending=False).\
        show()