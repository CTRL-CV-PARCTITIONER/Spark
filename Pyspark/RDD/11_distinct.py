# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 1), ("b", 3), ("a", 1)])
    # 去重算子, 可以当作是无参算子
    result = rdd.distinct()
    print(result.collect())