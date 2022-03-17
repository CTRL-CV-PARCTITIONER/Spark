# -*- coding: utf-8 -*-
import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':

    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    #读取文件
    file_rdd = sc.textFile("../input/demo", 2)
    #特殊字符list
    abnormal_char = [",",".","!","#","%","$"]
    #本地集合定义为广播变量
    broadcast = sc.broadcast(abnormal_char)
    #对特殊字符出现次数做累加, 定义累加器
    acmlt = sc.accumulator(0)
    count = 0
    #数据处理
    #1.切分数据
    split_rdd = file_rdd.flatMap(lambda x : x.split(" "))
    print(split_rdd.collect())
    #2.过滤空
    delete_space_rdd = split_rdd.filter(lambda x : x.strip())
    #过滤数据, 保留正常单词, 并对特殊单词做计数
    def filter_func(data):
        global acmlt, count
        #从广播变量中取出特殊字符
        abnormal_chars = broadcast.value
        if data in abnormal_chars:
            acmlt += 1
            count += 1
            print(acmlt, count)
            return False
        return True
    #3.得到正常单词list
    normal_worlds_rdd = delete_space_rdd.filter(filter_func)
    #4.将数据变形为元组, 便于计算
    result_rdd = normal_worlds_rdd.map(lambda x : (x, 1)). \
        reduceByKey(lambda a, b : a + b)
    print(result_rdd.collect())
    print(acmlt, count)