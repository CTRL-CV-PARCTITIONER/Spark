# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize(["hello world","hadoop spark","hello spark"])
    result = rdd.map(lambda x : x.split(" "))
    print(result.collect())
    # flatMap : map操作且解除嵌套
    result = rdd.flatMap(lambda x : x.split(" "))
    print(result.collect())