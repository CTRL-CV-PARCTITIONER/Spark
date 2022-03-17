# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':

    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("hdfs://localhost:8020/input/worlds.txt")
    #通过flatmap去除所有单词
    flat_res = rdd.flatMap(lambda x : x.split(" "))
    #将每一个单词变为元组
    map_res = flat_res.map(lambda x : (x, 1))
    #对数据进行分组聚合
    reduce_res = map_res.reduceByKey(lambda x, y : x + y)
    # 使用collect算子将数据手机到Driver中
    print(reduce_res.collect())