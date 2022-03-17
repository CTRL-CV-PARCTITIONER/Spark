# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1), ("b", 1), ("a", 1)], 3)
    # mapValues 只针对与 value
    result = rdd.mapValues(lambda x : x * 10)
    print(result.collect())