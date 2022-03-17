# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

if __name__ == '__main__':

    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    sc = spark.sparkContext
    #读取parqut类型文件
    #parqut时序列化后的文件无法直接查看, 安装插件可查看 setting-plugins-Avro and parqut view
    df = spark.read.format("parqut").load("../input/users.parqut")
    df.printSchema()
    df.show()