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

    #获取column对象
    name = df["name"]
    age = df["age"]

    #DLS风格
    df.select(["name","age"]).show()
    df.select("name","age").show()
    df.select(name, age).show()

    #filter API
    df.filter("age < 40").show()
    df.filter(df['age'] < 40).show()

    #where API
    df.where("age > 40").show()
    df.where(df['age'] > 40).show()

    #groupBy API
    df.groupBy("name").count().show()
    df.groupBy(df["name"]).count().show()

    r = df.groupBy("name")
    print(type(r))