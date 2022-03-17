# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':

    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()

    sc = spark.sparkContext

    schema = StructType().\
        add("name", StringType(), nullable=True).\
        add("age", StringType(), nullable=True).\
        add("job", StringType(), nullable=True)

    df = spark.read.format("csv").\
        option("sep", ";").\
        option("header", False).\
        option("encoding", "utf8").\
        schema(schema=schema).\
        load("../input/people.csv")

    #TODO:  write txt
    df.select(F.concat_ws("--", "name","age","job")).\
        write.\
        mode("overwrite").\
        format("text").\
        save("../input/text")

    #TODO: write csv
    df.write.\
        mode("overwrite").\
        format("csv").\
        option("sep", ";").\
        option('header', False).\
        save("../input/csv")

    #TODO: write json
    df.write.\
        mode("overwrite").\
        format("json").\
        save("../input/json")

    # TODO: write parquet
    df.write.\
        mode("overwrite").\
        format("parquet").\
        save("../input/parquet")