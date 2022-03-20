# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType

def split_line(x):
    return x.split(" ")

if __name__ == '__main__':

    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()

    sc = spark.sparkContext

    rdd = sc.parallelize([["hadoop flink spark"],["hadoop flink java"]])
    df = rdd.toDF(["line"])

    #TODO: 通过SparkSession构建udf对象
    udf1 = spark.udf.register("udf1", split_line, ArrayType(StringType()))

    # DSL
    df.select(udf1(df["line"])).show(truncate=False)

    #SQL
    df.createTempView("lines")
    spark.sql("select udf1(line) from lines").show(truncate=False)

    # functions 构建udf
    udf3 = F.udf(split_line, ArrayType(StringType()))
    df.select(udf3(df["line"])).show(truncate=False)