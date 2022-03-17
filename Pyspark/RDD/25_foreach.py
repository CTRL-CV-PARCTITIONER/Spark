# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([2, 5, 6, 3, 7, 9], 1)
    # foreach 修改数据不接受返回值
    res = rdd.foreach(lambda x : print(x * 10))