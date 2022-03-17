# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

def process(key):
    if key == "A":
        return 0
    elif key == "a":
        return 1
    else:
        return 2


if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9], 3)

    '''兩個是一個方法, repartition 底層調用coalesce， shuffle默认是ture'''

    #对rdd的分区进行重新分区(数量上)
    print(rdd.repartition(1).glom().collect())
    print(rdd.repartition(5).glom().collect())

    # coalesce 增加安全閥
    print(rdd.coalesce(1, shuffle=True).glom().collect())
    print(rdd.coalesce(5, shuffle=True).glom().collect())
