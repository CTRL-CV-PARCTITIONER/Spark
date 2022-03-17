# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([("A", 1), ("b", 2), ("a", 4), ("b", 3), ("a", 1)])
    # 只针对key排序
    result = rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key: str(key).lower())
    print(result.collect())