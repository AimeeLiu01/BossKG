#!/usr/bin/env python
# encoding: utf-8
'''
@author: liuchang
@software: PyCharm
@file: run.py
@time: 2020-06-18 16:08
'''
#-*- coding: UTF-8 -*-
import json
import os
import sys
import chardet
import time,datetime
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql import HiveContext,SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Row
from objectClean import objectClean
from raw2data import raw2data

def parserLawyer(x):
    result = []
    jdict = x
    if "P00000063" not in jdict.keys():
        return result
    if "P00000063" in jdict.keys():
        for i in range(len(jdict["P00000063"])):
            if "P00000065" not in jdict["P00000063"][i].keys():
                continue
            output = {}
            PeopleId = jdict["P00000063"][i]["P00000065"]
            output["@local_id"] = "Lawyer:" + PeopleId
            output["@type"] = ["person","lawyer"]
            #output["@source"] = jdict["@source"]
            for key in jdict["P00000063"][i].keys():
                if key != "P00000001" and key != "@updateTime" and key != "@source" and key != "@endTime" and key != "@startTime":
                    output[key] = jdict["P00000063"][i][key]
                if key == "P00000001":
                    output["P00000122"] = jdict["P00000063"][i][key]
            if output != {}:
                result.append((output["@local_id"], json.dumps(output, ensure_ascii=False)))
    return result


sc = SparkContext(appName='Lawyer')
hive_context= HiveContext(sc)
input_path = sys.argv[1]
output_path = sys.argv[2]
today = str(datetime.datetime.now().date())
raw = raw2data()

kgData = sc.textFile(input_path)
kgData = kgData.map(lambda x: raw.parserRawData(x.split('\t')[0])).filter(lambda x: x != "NONE" and x != 0)

kgData = kgData.map(lambda r : r.split('\t')[2]).map(json.loads).flatMap(lambda x : parserLawyer(x)).filter(lambda x: x != [])
lawyerData = kgData.map(lambda x: "\t".join([x[0], x[1]])).distinct().repartition(400)
lawyerData.saveAsTextFile(output_path+"/"+today)

sc.stop()
sys.stderr.write("^^^^^^^^^^^^^")