# -*- coding: utf-8 -*-
import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':

    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 2)

    #普通累加变量
    count = 0
    #spark提供的累加器变量, 分布式场景下完成数据统计累加
    acmlt = sc.accumulator(0)

    def map_count(data):
        global acmlt, count
        acmlt += 1
        count += 1
        print(acmlt)

    rdd.map(map_count).collect()
    print(acmlt)
    print(count)