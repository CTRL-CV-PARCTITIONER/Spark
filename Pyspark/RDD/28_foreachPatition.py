# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

def process(iter):
    print([i * 10 for i in iter])


if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    # foreachPartition ： foreach加强版， 性能更好 修改数据不接受返回值
    rdd.foreachPartition(process)