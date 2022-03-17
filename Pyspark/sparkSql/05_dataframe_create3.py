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
    #基于pandas的Dataframe 构建sparkDataframe
    data = pd.DataFrame(
        {
            "id":[1,2,3],
            "name":["张三","李四","王五"],
            "age":[11,33,44]
        }
    )

    df = spark.createDataFrame(data)
    df.printSchema()
    df.show()