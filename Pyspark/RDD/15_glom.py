# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd1 = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 2)
    # 使用glom算子, 查看每个分区内容
    print(rdd1.glom().collect())
