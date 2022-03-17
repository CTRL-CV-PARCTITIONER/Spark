# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':

    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([("a",1),("b",1),("a",1),("b",1),("a",1)], 3)
    # reduceByKey 分组聚合
    result = rdd.reduceByKey(lambda a, b : a + b)
    print(result.collect())