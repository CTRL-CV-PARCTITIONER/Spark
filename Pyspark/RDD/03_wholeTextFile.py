# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    #读取小文件夹
    rdd = sc.wholeTextFiles("../input/little_file")
    # print(rdd.collect())
    #将每一个元组中的的第二个值取出来
    print(rdd.map(lambda x: x[1]).collect())