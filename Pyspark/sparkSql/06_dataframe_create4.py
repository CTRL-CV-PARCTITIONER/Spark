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
    # 读取txt文件, 整行数据为一行没有分割
    schema = StructType().\
        add("a", StringType(), nullable=False)
    df = spark.read.format("text").\
        schema(schema=schema).\
        load("../input/worlds.txt")

    df.printSchema()
    df.show()