#!/usr/bin/env python
# encoding: utf-8
'''
@author: liuchang
@software: PyCharm
@file: usedata-0506.py
@time: 2020-06-18 15:56
'''
# -*- coding: utf-8 -*-
import json
import os
import sys
import re
import time,datetime
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('./')


if __name__ == "__main__":
    for line in sys.stdin:
        kgid = line[:-1].split('\t')[0]
        jdict = json.loads(line[:-1].split('\t')[1])
        output = {}
        if "P00000109" in jdict.keys():
            meituanId = jdict["P00000109"]["@value"][0]["P00000106"]
            meituanId = meituanId.split("mtId=")[1]
            #output["@source"] = jdict["P00000109"]["@value"][0]["@source"]
            output["@local_id"] = "Meituan:" + meituanId
            output["@type"] = ["organization","shop"]
            #output["@creatTime"] = str(datetime.datetime.now().date())
            for key in jdict["P00000109"]["@value"][0].keys():
                if key != "@updateTime" and key != "@source" and key != "P00000001":
                    output[key] = jdict["P00000109"]["@value"][0][key]
                if key == "P00000001":
                    output["P00000118"] = jdict["P00000109"]["@value"][0][key]

            name = ""
            comid = ""
            if len(line[:-1].split('\t')) == 3:
                jdict2 = json.loads(line[:-1].split('\t')[2])
                if "P00000001" in jdict2.keys():
                    name = jdict2["P00000001"]["@value"]
                    comid = jdict2["@kg_id"]
            output["P00000119"] = []
            if name != "" and comid != "":
                new_dic = {"id": comid ,"name": name}
                output["P00000119"].append(new_dic)

            if output != {}:
                print(output["@local_id"] + "\t" + json.dumps(output, ensure_ascii=False).encode('utf8'))
