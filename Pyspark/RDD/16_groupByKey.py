# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd1 = sc.parallelize([("a", 1), ("b", 2), ("a", 1), ("b", 3), ("a", 1)])
    # groubykey 分组, 与group 一样
    result = rdd1.groupByKey()
    res = rdd1.groupBy(lambda x : x[0])

    print(result.map(lambda x: (x[0], list(x[1]))).collect())
    print(res.map(lambda x: (x[0], list(x[1]))).collect())
