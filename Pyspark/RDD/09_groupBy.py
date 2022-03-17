# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 1), ("b", 3), ("a", 1)])
    #通过groupBy对数据进行分组, 每个元组的第一个数据为分组对象
    result = rdd.groupBy(lambda x : x[0])
    #将对象强转为list
    result = result.map(lambda x : (x[0], list(x[1])))
    print(result.collect())