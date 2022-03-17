# -*- coding: utf-8 -*-
import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':

    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.textFile("../input/worlds.txt")
    rdd2 = rdd1.flatMap(lambda x : x.split(" "))
    rdd3 = rdd2.map(lambda x : (x, 1))

    rdd3.cache()  #将rdd3保存在缓存中
    #手动设置缓存级别
    rdd3.persist(StorageLevel.MEMORY_ONLY)

    rdd4 = rdd3.reduceByKey(lambda a, b : a + b)
    print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x : sum(x))
    print(rdd6.collect())

    rdd3.unpersist()  #将rdd3从缓冲中清除