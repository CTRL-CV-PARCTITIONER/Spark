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

    #TODO:数据写入mysql
    df.write.\
        mode("overwrite").\
        format("jdbc").\
        option("url","jdbc:mysql://localhost:3306/qfdb?useSSL=false&useUnicode-true").\
        option("dbtable","data").\
        option("user","root").\
        option("password","admin").\
        save()

    # TODO:读取mysql数据
    data = spark.read.format("jdbc"). \
        option("url", "jdbc:mysql://localhost:3306/qfdb?useSSL=false&useUnicode-true"). \
        option("dbtable", "emp"). \
        option("user", "root"). \
        option("password", "admin"). \
        load()

    data.printSchema()
    data.show()