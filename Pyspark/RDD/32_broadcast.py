# -*- coding: utf-8 -*-
import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':

    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    local_list = [
        (1, "大仙", 11),
        (2, "小燕", 13),
        (3, "甜甜", 11),
        (4, "大力", 11),
    ]
    #将本地list标记为广播变量
    broad = sc.broadcast(local_list)

    score_rdd = sc.parallelize(
        [
            (1, "语文", 99),
            (2, "数学", 99),
            (3, "英语", 99),
            (4, "编程", 99),
            (1, "语文", 99),
            (2, "编程", 99),
            (3, "语文", 99),
            (4, "英语", 99),
            (1, "语文", 99),
            (1, "英语", 99),
            (1, "编程", 99),
        ]
    )

    #
    def map_func(data):
        id = data[0]
        #在使用到本地list的地方, 从广播变量中直接取出即可 broad.value
        name = [local[1] for local in broad.value if id == local[0]][0]
        return (name, data[1], data[2])

    result_rdd = score_rdd.map(map_func)
    print(result_rdd.collect())










