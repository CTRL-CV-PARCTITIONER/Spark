# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd1 = sc.parallelize([("a", 1), ("b", 2), ("c", 1)])
    rdd2 = sc.parallelize([("a", 5), ("b", 6),("d","wei")])
    #join算子 rdd连接操作, 只能用于二元元组(k,v), 连接条件按照key进行关联
    '''内连接'''
    res1 = rdd1.join(rdd2)
    print(res1.collect())
    '''左连接'''
    res2 = rdd1.leftOuterJoin(rdd2)
    print(res2.collect())
    '''右链接'''
    res3 = rdd1.rightOuterJoin(rdd2)
    print(res3.collect())