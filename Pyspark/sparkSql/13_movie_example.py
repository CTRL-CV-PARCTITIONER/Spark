# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as F

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName("movie"). \
        master("local[*]"). \
        getOrCreate()

    sc = spark.sparkContext

    schema = StructType(). \
        add("user_id", StringType(), nullable=True). \
        add("movie_id", StringType(), nullable=True). \
        add("rank", IntegerType(), nullable=True). \
        add("ts", StringType(), nullable=True)

    df = spark.read.format("csv"). \
        option("sep", "\t"). \
        option("header", False). \
        option('encoding', 'utf8'). \
        schema(schema=schema). \
        load("../input/u.data")

    # TODO 1: 用户平均分
    df.groupBy("user_id"). \
        avg("rank"). \
        withColumnRenamed("avg(rank)", "avg_rank"). \
        withColumn("avg_rank", F.round("avg_rank", 2)). \
        orderBy("avg_rank", ascending=False). \
        show()

    # TODO 2: 电影平均分
    df.createTempView("movie")
    spark.sql(
        "select movie_id, round(avg(rank)) as avg_rank from movie group by movie_id order by avg_rank desc"
    ).show()

    # TODO 3: 查询大于平均分的电影的数量
    print(df.where(df["rank"] > df.select(F.avg(df["rank"])).first()["avg(rank)"]).count())

    # TODO 4: 查询高分电影中(>3)打分次数最多的用户, 此人打分的平均分
    id = df.where(df["rank"] > 3). \
        groupBy("user_id"). \
        count(). \
        withColumnRenamed("count", "cnt"). \
        orderBy("cnt", ascending=False). \
        limit(1). \
        first()["user_id"]

    # df.where(df["user_id"] == id). \
    #     groupBy("user_id"). \
    #     avg(). \
    #     withColumnRenamed("age(rank)", "avg_rank").\
    #     show()
    df.where(df["user_id"] == id). \
        select(F.round(F.avg("rank"), 2)). \
        withColumnRenamed("round(avg(rank), 2)", "avg_rank"). \
        show()

    # TODO 5: 查询每个用户的平均打分、最高打分、最低打分
    # spark.sql(
    #     "select user_id, round(avg(rank),2) avg_rank, max(rank) max_rank, min(rank) min_rank from movie group by user_id"). \
    #     show()
    df.groupBy("user_id"). \
        agg(
        F.round(F.avg("rank"), 2).alias("avg_rank"),
        F.max("rank").alias("max_rank"),
        F.min("rank").alias("min_rank")
    ).show()

    # TODO 6: 查询评分超过100次的电影的平均分、排名、TOP10
    df.groupBy("movie_id"). \
        agg(
        F.count("rank").alias("cnt"),
        F.round(F.avg("rank"), 2).alias("avg")
    ).where("cnt > 100").\
        orderBy("avg", ascending=False).\
        limit(10).show()
