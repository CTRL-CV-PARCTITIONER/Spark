# -*- coding: utf-8 -*-


from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 初始化执行环境, 构建sparkConf对象, 给定程序名称, 默认是本地运行获取本地最大资源
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    # 创建sparkcontext 对象
    sc = SparkContext(conf=conf)
    # 通过并行化集合的方式创建RDD
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    # parallelize获取分区数量, 不指定分区数, 默认根据CPU核心来定
    print("默认分区数:", rdd.getNumPartitions())
    rdd = sc.parallelize([1, 2, 3], 3)
    print("指定分区数:", rdd.getNumPartitions())
    # collect方法是将RDD中每个分区的数据都发送到driver中,形成一个python list对象
    print("RDD内容:", rdd.collect())