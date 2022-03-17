# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

def add(data):
    return data * 10

if __name__ == '__main__':

    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1,2,3,4,5,6,7,8], 3)
    #定义方法作为算子的传入函数体
    result = rdd.map(add)
    print(result.collect())
    #定义lambda表达式 (匿名函数)
    result = rdd.map(lambda x : x + 100)
    print(result.collect())