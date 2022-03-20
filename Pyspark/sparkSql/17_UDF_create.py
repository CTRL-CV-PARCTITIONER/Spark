# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, IntegerType

def num_ride(x):
    return x * 10

if __name__ == '__main__':

    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()

    sc = spark.sparkContext

    rdd = sc.parallelize([1,2,3,4,5,6,7]).\
        map(lambda x : [x])

    df = rdd.toDF(["num"])

    #TODO 1: 通过sparkSession对象注册
    udf2 = spark.udf.register("udf1", num_ride, IntegerType())


    #TODO 1.1: sql风格使用
    # 以select的表达式执行
    df.selectExpr("udf1(num)").show()

    #TODO 1.2: DSL风格
    df.select(udf2(df["num"])).show()


    #TODO 2:注册方式2
    udf3 = F.udf(num_ride, IntegerType())

    #TODO 2.1: 仅能用于DSL风格
    df.select(udf3(df["num"])).show()