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
    rdd = sc.parallelize([1,2,3,4,5], 3)
    df = rdd.map(lambda x : [x]).toDF(["num"])
    sing = df.rdd.repartition(1)

    #mapPartitions 放大的返回值要求是list
    result = sing.mapPartitions(process).collect()
    print(result)