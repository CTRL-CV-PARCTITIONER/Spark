# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 1), ("b", 3), ("a", 1)])
    rdd1 = sc.parallelize([("a", 5), ("b", 6), ("a", 7), ("b", 8), ("a", 9)])
    rdd2 = sc.parallelize([1,2,3,4,5,6,7])
    #union算子 将两个rdd合并 , 不会去重, rdd类型不同可以合并
    result = rdd.union(rdd1)
    res = result.union(rdd2)
    print(res.collect())