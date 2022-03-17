# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9], 1)
    # 随机抽取内容, 都去过了就不会再取
    print(rdd.takeSample(False, 22))