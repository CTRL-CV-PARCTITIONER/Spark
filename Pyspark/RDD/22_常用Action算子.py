# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    sc.parallelize([1, 2, 3]).first()  #取第一个值
    sc.parallelize([1, 2, 3]).take(5)  #取前5个值
    sc.parallelize([1, 2, 3, 4, 5]).top(3)  #降序排序取前3个, 取最大的几条数据
    sc.parallelize([1, 2, 3, 4, 5]).count() #有几条数据