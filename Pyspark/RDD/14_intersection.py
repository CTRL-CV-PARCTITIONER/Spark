# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd1 = sc.parallelize([("a", 1), ("b", 2), ("c", 1)])
    rdd2 = sc.parallelize([("a", 1), ("b", 6),("d","wei")])
    #通过intersection算子求交集
    result = rdd1.intersection(rdd2)
    print(result.collect())