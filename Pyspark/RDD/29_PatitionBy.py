# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

def process(key):
    if key == "A":
        return 0
    elif key == "a":
        return 1
    else:
        return 2


if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([("A", 1), ("b", 2), ("a", 4), ("b", 3), ("a", 1)])
    #自定义分区
    res = rdd.partitionBy(3, process)

    print(res.glom().collect())