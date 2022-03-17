# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 1), ("b", 3), ("a", 1)])
    #过滤想要的数据进行保留
    result = rdd.filter(lambda x : x[1] > 1)
    print(result.collect())