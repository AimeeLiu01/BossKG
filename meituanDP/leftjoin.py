#!/usr/bin/env python
# encoding: utf-8
'''
@author: liuchang
@software: PyCharm
@file: leftjoin.py
@time: 2020-06-18 15:56
'''
#-*- coding: UTF-8 -*-
import json
import os
import sys
import chardet
import datetime
from datetime import datetime
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql import HiveContext
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Row

sc = SparkContext(appName='DPMapping')
hive_context= HiveContext(sc)
input_path1 = sys.argv[1]
input_path2 = sys.argv[2]
outPath = sys.argv[3]
outPath2 = sys.argv[4]

meituanData = sc.textFile(input_path1)
kgData = sc.textFile(input_path2)
meituanData = meituanData.map(lambda x : Row(kgid=x.split("\t")[0], jdict=x.split("\t")[1]))
kgData = kgData.map(lambda x : Row(kgid=x.split("\t")[0], jdict=x.split("\t")[1]))
meituanData = meituanData.toDF()
kgData = kgData.toDF()
Joined = meituanData.join(kgData, "kgid")
notJoined = meituanData.select("kgid").subtract(kgData.select("kgid"))
notJoined2 = meituanData.join(notJoined, "kgid")

Joined = Joined.rdd
notJoined2 = notJoined2.rdd
print("joined")
print(Joined.take(5))
print("notJoined")
print(notJoined2.take(5))

Joined.map(lambda x : "\t".join([x[0], x[1], x[2]]).encode('utf8')).distinct().saveAsTextFile(outPath)
notJoined2.map(lambda x : "\t".join([x[0], x[1]]).encode('utf8')).distinct().saveAsTextFile(outPath2)
sc.stop()
sys.stderr.write("^^^^^^^^^^^^^")