# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("../input/worlds.txt")
    # print(rdd.collect())
    # 写数据, 可以写到本地和hdfs
    rdd.saveAsTextFile("hdfs://192.168.223.100:8020/input/out1")