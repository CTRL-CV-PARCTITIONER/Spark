# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([2, 5, 6, 3, 7, 9], 1)
    # 选取最大或最小的 n个数据， 比top更优
    print(rdd.takeOrdered(400))
    print(rdd.takeOrdered(400, lambda x: -x))
