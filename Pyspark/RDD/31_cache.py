# -*- coding: utf-8 -*-

'''
    MEMORY_ONLY: 使用未序列化的Java对象格式，将数据保存在内存中。如果内存不够存放所有的数据，则某些分区
    的数据就不会进行持久化。那么下次对这个RDD执行算子操作时，那些没有被持久化的数据，需要从源头处重新计算一遍。
    这是默认的持久化策略，使用cache()方法时，实际就是使用的这种持久化策略。
    
    MEMORY_ONLY_SER: 基本含义同MEMORY_ONLY。唯一的区别是，会将RDD中的数据进行序列化，RDD的每个partition
    会被序列化成一个字节数组。这种方式更加节省内存，从而可以避免持久化的数据占用过多内存导致频繁GC。
    
    MYMORY_AND_DISK: 使用未序列化的Java对象格式，优先尝试将数据保存在内存中。如果内存不够存放所有的数据，
    会将数据写入磁盘文件中，下次对这个RDD执行算子时，持久化在磁盘文件中的数据会被读取出来使用。
    
    MEMORY_AND_DISK_SER: 基本含义同MEMORY_AND_DISK。唯一的区别是，会将RDD中的数据进行序列化，RDD的每个
    partition会被序列化成一个字节数组。这种方式更加节省内存，从而可以避免持久化的数据占用过多内存导致频繁GC。
    
    DISK_ONLY: 使用未序列化的Java对象格式，将数据全部写入磁盘文件中。
    
    MEMORY_ONLY_2,MEMORY_AND_DISK_2: 对于上述任意一种持久化策略，如果加上后缀_2，代表的是将每个持久化的数据，
    都复制一份副本，并将副本保存到其他节点上。这种基于副本的持久化机制主要用于进行容错。假如某个节点挂掉，节点的内存
    或磁盘中的持久化数据丢失了，那么后续对RDD计算时还可以使用该数据在其他节点上的副本。如果没有副本的话，就只能将这些数据从源头处重新计算一遍了。
    
    OFF_HEAP(experimental): RDD的数据序例化之后存储至Tachyon。相比于MEMORY_ONLY_SER，OFF_HEAP能够减少垃圾回收开销、
    使得Spark Executor更“小”更“轻”的同时可以共享内存；而且数据存储于Tachyon中，Spark集群节点故障并不会造成数据丢失，
    因此这种方式在“大”内存或多并发应用的场景下是很有吸引力的。需要注意的是，Tachyon并不直接包含于Spark的体系之内，
    需要选择合适的版本进行部署；它的数据是以“块”为单位进行管理的，这些块可以根据一定的算法被丢弃，且不会被重建。
'''


import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':

    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.textFile("../input/worlds.txt")
    rdd2 = rdd1.flatMap(lambda x : x.split(" "))
    rdd3 = rdd2.map(lambda x : (x, 1))

    rdd3.cache()  #将rdd3保存在缓存中
    #手动设置缓存级别
    rdd3.persist(StorageLevel.MEMORY_ONLY)

    rdd4 = rdd3.reduceByKey(lambda a, b : a + b)
    print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x : sum(x))
    print(rdd6.collect())

    rdd3.unpersist()  #将rdd3从缓冲中清除
