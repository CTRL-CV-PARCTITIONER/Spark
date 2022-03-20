# -*- coding: utf-8 -*-
import string

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType, MapType

def process(x):
    sum = 0
    for i in x:
        sum += i["num"]
    return [sum]


if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        config("spark.sql.shuffle.partitions", 2). \
        getOrCreate()

    sc = spark.sparkContext
    #python 只支持udf， 不支持udaf,
    rdd = sc.parallelize([
        (1, "语文", 92),
        (2, "数学", 95),
        (3, "英语", 29),
        (4, "编程", 59),
        (1, "语文", 79),
        (2, "编程", 19),
        (3, "语文", 99),
        (4, "英语", 95),
        (1, "语文", 91),
        (1, "英语", 90),
        (1, "英语", 90),
        (1, "编程", 85),
    ])

    shcema = StructType().\
        add("id", IntegerType(), nullable=True). \
        add("class", StringType(), nullable=True). \
        add("score", StringType(), nullable=True)

    df = rdd.toDF(schema=shcema)

    df.createTempView("test")

    spark.sql("""
        select * ,
        row_number() over(order by score) as row_num,
        dense_rank() over(partition by class order by score) as dense_num,
        rank() over(partition by class order by score) as rank_num
        from test
    """).show();