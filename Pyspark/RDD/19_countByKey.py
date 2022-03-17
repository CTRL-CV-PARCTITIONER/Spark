# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':

    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("../input/worlds.txt")
    rdd1 = rdd.flatMap(lambda x: x.split(" ")).map(lambda x : (x, 1))
    #countByKey 进行计数
    res = rdd1.countByKey()
    print(res)