# -*- coding: utf-8 -*-
import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':

    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    #告知spark开启checkpoint功能
    sc.setCheckpointDir("hdfs://localhost:8020/input/checkpoint")
    rdd1 = sc.textFile("../input/worlds.txt")
    rdd2 = rdd1.flatMap(lambda x : x.split(" "))
    rdd3 = rdd2.map(lambda x : (x, 1))

    #调用checkpoint API， 保存数据即可
    rdd3.checkpoint()

    rdd4 = rdd3.reduceByKey(lambda a, b : a + b)
    print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x : sum(x))
    print(rdd6.collect())

    rdd3.unpersist()  #将rdd3从缓冲中清除