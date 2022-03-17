# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, IntegerType, StringType

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        config("spark.sql.shuffle.partitions", 2). \
        getOrCreate()

    sc = spark.sparkContext

    schema = StructType(). \
        add("name", StringType(), nullable=True). \
        add("age", StringType(), nullable=True). \
        add("job", StringType(), nullable=True)

    df = spark.read.format("csv"). \
        option("sep", ";"). \
        option("header", False). \
        schema(schema=schema). \
        load("../input/people.csv")

    # TODO: 数据去重
    # df.drop_duplicates(["name","age"]).orderBy("age", ascending=True).show()

    # TODO: 缺失值删除
    # df.dropna().show()
    # df.dropna(subset=["name", "age"]).show()
    # df.dropna(thresh=2, subset=["name", "age"]).show()

    # TODO: 缺失值填充
    # df.fillna("loss").show()
    # df.fillna("loss", subset=["age"]).show()
    # df.fillna({"age": "loss", "job": "success"}).show()