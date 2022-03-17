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
    #读取csv数据
    df = spark.read.format("csv").\
        option("sep",";").\
        option("header",True).\
        option("index",True).\
        option("encoding","utf8").\
        schema("name STRING, age INT, job STRING").\
        load("../input/people.csv")

    df.printSchema()
    df.show()