# -*- coding: utf-8 -*-
import string

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType, MapType


def split_line(x):
    return x.split(" ")


if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        config("spark.sql.shuffle.partitions", 2). \
        getOrCreate()

    sc = spark.sparkContext

    # TODO:  返回字典
    rdd = sc.parallelize([[1], [2], [3]])
    df = rdd.toDF(["num"])


    def process(x):
        return {"num": x, "letters": string.ascii_letters[x]}

    udf2 = spark.udf.register("udf1", process,
                              StructType().\
                              add("num",IntegerType(), nullable=True).\
                              add("letters", StringType(), nullable=True))

    df.selectExpr("udf1(num)").show()

    df.select(udf2(df["num"])).show()
