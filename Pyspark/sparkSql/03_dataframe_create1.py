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

    # 构建表结构的描述对象, StructType对象
    schme = StructType().add("a",StringType(), nullable=False).\
        add("b", IntegerType(), nullable=False).\
        add("c", StringType(), nullable=False)

    df = spark.createDataFrame(rdd, schema=schme)
    df.printSchema()
    df.show()