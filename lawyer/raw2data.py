#!/usr/bin/env python
# encoding: utf-8
'''
@author: liuchang
@software: PyCharm
@file: raw2data.py
@time: 2020-06-18 16:10
'''
# -*- coding: UTF-8 -*-
import json
import os
import sys
import chardet
import time
import datetime
import copy
reload(sys)
sys.setdefaultencoding('utf-8')

sys.path.append('./')
from objectClean import objectClean

#将输入数据KG实体化
class raw2data():

    def __init__(self):
        self.SM = {} #schema模型
        self.outputData = {} #输出
        self.flag = str(2) #2:根据本次爬取数据建立的KG实体，1:KG库中已有的KG实体
        self.clean = objectClean("schema_clean.conf") #数据清洗，格式规范
        self.readSchemaMappingConf("schema_mapping.conf") #SM初始化

    #读取实体schema，数据建立实体的模版
    def readSchemaMappingConf(self, add):
        fd = open(add)
        for line in fd:
            lineArr = line[:-1].split("\t")
            self.SM[lineArr[0]] = lineArr[1]
        fd.close()

    #将数据转为KG实体
    def parserRawData(self, line):
        try:
            jdict = json.loads(line)
        except:
            return 0

	self.flag = '2'
        self.outputData = {}

        if "crawler_task_info" not in jdict:
            sys.stderr.write("crawler_task_info not in jdict!\n")
            return 0

        if "task_id" not in jdict and jdict["crawler_task_info"]["modelName"] != "hkgsxt:basic_enterprise_info":
            sys.stderr.write("cannot find task id\n")
            return 0

        self.outputData["@kg_id"] = jdict["crawler_task_info"]["kg_id"]
        self.outputData["@crawler_time"] = jdict["crawler_task_info"]["crawler_time"]
        self.outputData["@source"] = jdict["crawler_task_info"]["source"]
	self.outputData["@type"] = "registrationCompany"

	if "12348" in  jdict["crawler_task_info"]["modelName"]:
	    self.outputData["@type"] = "lawfirm"

        if jdict["crawler_task_info"]["modelName"] == "12348:basic_lawyer_info":
            for key in jdict["crawler_task_info"]:
                if key in self.SM:
                    self.outputData[self.SM[key]] = jdict["crawler_task_info"][key]

            for key in jdict["basic_lawyer_info"]:
                if key in self.SM:
                    if key == "terms":
                        self.outputData[self.SM[key]] = []
                        for lawer in jdict["basic_lawyer_info"][key]:
                            lawer_cache = {}
                            for kl in lawer:
                                if kl in self.SM:
                                    lawer_cache[self.SM[kl]] = lawer[kl]
                            self.outputData[self.SM[key]].append(lawer_cache)
                        continue
                    self.outputData[self.SM[key]] = jdict["basic_lawyer_info"][key]

        if jdict["crawler_task_info"]["modelName"] == "gsxt:major_person":
            self.outputData[self.SM["major_person"]] = []
            for person in jdict["major_person"]:
                person_cache = {}
                for key in person:
                    if key in self.SM:
                        person_cache.setdefault(self.SM[key],person[key])
                self.outputData[self.SM["major_person"]].append(person_cache)

        self.outputData = self.clean.cleanData2(self.outputData)
        if "P00000063" in self.outputData:
            return self.outputData["@kg_id"] + '\t' + self.flag + '\t' + json.dumps(self.outputData, ensure_ascii=False)
        return "NONE"
#        return self.outputData["@kg_id"] + '\t' + self.flag + '\t' + json.dumps(self.outputData, ensure_ascii=False)

if __name__ == "__main__":
    oc = raw2data()
    for line in sys.stdin:
        kg_json = line[:-1]
        res = oc.parserRawData(kg_json)
