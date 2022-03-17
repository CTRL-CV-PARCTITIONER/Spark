# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    #构建conf对象
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    #构建sparkcontext对象
    sc = SparkContext(conf=conf)
    #通过textFile 读取文件数据
    file_rdd1 = sc.textFile("../input/worlds.txt")
    #获取默认rdd分区数
    print("默认分区数:", file_rdd1.getNumPartitions())
    print("file_rdd1内容:", file_rdd1.collect())

    #设置最小分区数测试
    file_rdd2 = sc.textFile("../input/worlds.txt", 3)
    file_rdd3 = sc.textFile("../input/worlds.txt", 100)
    print("file_rdd2分区数:", file_rdd2.getNumPartitions())
    print("file_rdd3分区数:", file_rdd3.getNumPartitions())


    #读取hdfs文件
    file_rdd4 = sc.textFile("hdfs://localhost:8020/input/worlds.txt")
    print("file_rdd4分区数:", file_rdd4.getNumPartitions())
    print("file_rdd4内容:", file_rdd4.collect())