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
    
    存储级别的选择：
    MEMORY_ONLY > MEMORY_ONLY_SER > MEMORY_AND_DISK_SER > MEMORY_AND_DISK

    默认情况下，性能最高的当然是MEMORY_ONLY，但前提是你的内存必须足够足够大，可以绰绰有余地存放下整个RDD的所有数据。因为不进行序列化与反序列化操作，
    就避免了这部分的性能开销；对这个RDD的后续算子操作，都是基于纯内存中的数据的操作，不需要从磁盘文件中读取数据，性能也很高；而且不需要复制一份数据副本，
    并远程传送到其他节点上。但是这里必须要注意的是，在实际的生产环境中，恐怕能够直接用这种策略的场景还是有限的，如果RDD中数据比较多时（比如几十亿），
    直接用这种持久化级别，会导致JVM的OOM内存溢出异常。

    如果使用MEMORY_ONLY级别时发生了内存溢出，那么建议尝试使用MEMORY_ONLY_SER级别。该级别会将RDD数据序列化后再保存在内存中，
    此时每个partition仅仅是一个字节数组而已，大大减少了对象数量，并降低了内存占用。这种级别比MEMORY_ONLY多出来的性能开销，
    主要就是序列化与反序列化的开销。但是后续算子可以基于纯内存进行操作，因此性能总体还是比较高的。此外，可能发生的问题同上，
    如果RDD中的数据量过多的话，还是可能会导致OOM内存溢出的异常。

    如果纯内存的级别都无法使用，那么建议使用MEMORY_AND_DISK_SER策略，而不是MEMORY_AND_DISK策略。因为既然到了这一步，就说明RDD的数据量很大，
    内存无法完全放下。序列化后的数据比较少，可以节省内存和磁盘的空间开销。同时该策略会优先尽量尝试将数据缓存在内存中，内存缓存不下才会写入磁盘。

    通常不建议使用DISK_ONLY和后缀为_2的级别：因为完全基于磁盘文件进行数据的读写，会导致性能急剧降低，
    有时还不如重新计算一次所有RDD。后缀为_2的级别，必须将所有数据都复制一份副本，并发送到其他节点上，
    数据复制以及网络传输会导致较大的性能开销，除非是要求作业的高可用性，否则不建议使用。
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
