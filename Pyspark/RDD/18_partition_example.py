# -*- coding: utf-8 -*-
import json

from pyspark import SparkConf, SparkContext
import os
from test import city

os.environ["HADOOP_CONF_DIR"] = "/opt/software/hadoop/etc/hadoop"

if __name__ == '__main__':
    conf = SparkConf().setAppName("test_yarn1").setMaster("yarn")
    conf.set("spark.submit.pyFiles", "test.py")
    sc = SparkContext(conf=conf)
    textfile = sc.textFile("hdfs://localhost:8020/input/jsondata.txt")
    splitfile = textfile.flatMap(lambda x : x.split("|"))
    jsonfile = splitfile.map(lambda x : json.loads(x))
    filterfile = jsonfile.filter(lambda x : x["a"] == "100")
    strfile = filterfile.map(city)
    print(strfile.collect())
    distinctfile = strfile.distinct()
    print(distinctfile.collect())