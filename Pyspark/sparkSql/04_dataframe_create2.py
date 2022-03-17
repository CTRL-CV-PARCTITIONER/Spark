# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':

    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    sc = spark.sparkContext
    rdd = sc.textFile("../input/worlds.txt"). \
        map(lambda x: x.split(" ")). \
        map(lambda x: (x[0], int(x[1]), x[2]))

    #toDF构建Dataframe
    df = rdd.toDF(["a","b","c"])
    df.printSchema()
    df.show()

