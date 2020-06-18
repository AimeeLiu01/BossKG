#!/usr/bin/env python
# encoding: utf-8
'''
@author: liuchang
@software: PyCharm
@file: objectClean.py.py
@time: 2020-06-18 14:44
@python 2.7语法编写而成
'''

import json
import os
import sys
import re
import datetime
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('./')
from langconv import *


#数据清洗，格式规范
class objectClean():

    def __init__(self, schemaCleanConfAdd):
        self.pdict = {}
        self.readSchemaCleanConf(schemaCleanConfAdd)

    #读取schema
    def readSchemaCleanConf(self, add):
        fd = open(add)
        for line in fd:
            lineArr = line[:-1].split("\t")
            if lineArr[1] not in self.pdict:
                self.pdict[lineArr[1]] = []
            self.pdict[lineArr[1]].append(lineArr[0])
        fd.close()


    def cleanData(self, jdict):

        # Step1: 处理空值
        for key in list(jdict.keys()):
            if key == 'P00000050' or key == 'P00000081' or key == 'P00000095' or key == 'P00000031':
                continue
            # 崔鹏代码
            if type(jdict[key]) != dict:
                if jdict[key] == "":
                    jdict[key] = "NONE"
                if jdict[key] == " ":
                    jdict[key] = "NONE"
                if jdict[key] == "None":
                    jdict[key] = "NONE"
                if jdict[key] is None:
                    del jdict[key]
                    continue
                if jdict[key] == "NONE":
                    continue
                elif key in self.pdict["date"]:
                    jdict[key] = self.dateCleaner(jdict[key])
                elif key in self.pdict["RegistCapi"]:
                    jdict[key] = self.moneyCleaner(jdict[key])

                if type(jdict[key]) != list:
                    if len(re.findall(r'^</?\w+[^>]*>', jdict[key])) > 0:
                        del jdict[key]

                elif type(jdict[key]) == list:
                    for kk in jdict[key]:
                        for ks in kk.keys():
                            if type(kk[ks]) == str:
                                if len(re.findall(r'^</?\w+[^>]*>', kk[ks])) > 0:
                                    del kk[ks]
                                if "img" and "src" in kk[ks]:
                                    del kk[ks]
            else:

                # @刘畅，处理history中的空值
                # 说明主键对应的类型为dict,这时候一定有history
                for key1 in jdict[key].keys():
                    if key1 == '@history':
                        # 判断history的长度，his_len
                        # 进行循环，处理每条history
                        if jdict[key][key1] == "":
                            jdict[key][key1] = "NONE"
                        elif jdict[key][key1] == "null":
                            jdict[key][key1] = "NONE"
                        elif jdict[key][key1] == " ":
                            jdict[key][key1] = "NONE"
                        elif jdict[key][key1] == "None":
                            jdict[key][key1] = "NONE"
                        elif jdict[key][key1] is None:
                            del jdict[key][key1]
                            continue
                        elif type(jdict[key]['@history']) == list and len(jdict[key]['@history']) == 0:
                            del jdict[key]
                            break
                        else:
                            for i in range(len(jdict[key]['@history'])):
                                # hist_tmp = jdict[key]['@history'][i]
                                for key2 in jdict[key]['@history'][i].keys():
                                    if jdict[key]['@history'][i][key2] == "":
                                        jdict[key]['@history'][i][key2] = "NONE"
                                    if jdict[key]['@history'][i][key2] == " ":
                                        jdict[key]['@history'][i][key2] = "NONE"
                                    if jdict[key]['@history'][i][key2] == "None":
                                        jdict[key]['@history'][i][key2] = "NONE"
                                    if jdict[key]['@history'][i][key2] == "null":
                                        jdict[key]['@history'][i][key2] = "NONE"
                                    if  jdict[key]['@history'][i][key2] is None:
                                        del jdict[key]['@history'][i][key2]
                                        continue


                    if key1 == '@value':
                        if type(jdict[key]['@value']) == list:
                            for i in range(len(jdict[key]['@value'])):
                                if type(jdict[key]['@value'][i]) == dict:
                                    for key3 in jdict[key]['@value'][i].keys():
                                        if jdict[key]['@value'][i][key3] == "":
                                            jdict[key]['@value'][i][key3] = "NONE"
                                        if jdict[key]['@value'][i][key3] == " ":
                                            jdict[key]['@value'][i][key3] = "NONE"
                                        if jdict[key]['@value'][i][key3] == "None":
                                            jdict[key]['@value'][i][key3] = "NONE"
                                        if jdict[key]['@value'][i][key3] == "null":
                                            jdict[key]['@value'][i][key3] = "NONE"
                                        if jdict[key]['@value'][i][key3] is None:
                                            del jdict[key]['@value'][i][key3]
                                            continue

                        elif type(jdict[key]['@value']) == unicode and type(jdict[key]['@value'].encode("utf-8")) == str:
                            if jdict[key]['@value'] == "":
                                jdict[key]['@value'] = "NONE"
                            if jdict[key]['@value'] == " ":
                                jdict[key]['@value'] = "NONE"
                            if jdict[key]['@value'] == "None":
                                jdict[key]['@value'] = "NONE"
                            if jdict[key]['@value'] == "null":
                                jdict[key]['@value'] = "NONE"

                        elif jdict[key]['@value'] is None:
                            del jdict[key]['@value']
                            continue



        # Step2：处理string类型，处理中英文符号，全角转半角，繁体转简体，名字中的空格等
        for key in list(jdict.keys()):
            if type(jdict[key]) == dict:
                # 情况一：该字段有Value，并且Value的类型为string
                if '@value' in jdict[key].keys() and type(jdict[key]['@value']) == unicode:
                    if type(jdict[key]['@value'].encode("utf-8")) == str:
                        jdict[key]['@value'] = self.symbolCleaner(jdict[key]['@value'])
                        jdict[key]['@value'] = self.Traditional2Simplified(jdict[key]['@value'])
                        jdict[key]['@value'] = self.full2half(jdict[key]['@value'])
                        jdict[key]['@value'] = self.nameCleaner(jdict[key]['@value'])

                # 情况二：该字段有History，且History中存在value字段
                if '@history' in jdict[key].keys() and type(jdict[key]['@history']) == list:
                    for i in range(len(jdict[key]['@history'])):
                        if type(jdict[key]['@history'][i]) == dict:
                            if '@value' in jdict[key]['@history'][i].keys() and type(
                                    jdict[key]['@history'][i]['@value']) == unicode:
                                if type(jdict[key]['@history'][i]['@value'].encode("utf-8")) == str:
                                    jdict[key]['@history'][i]['@value'] = self.symbolCleaner(
                                        jdict[key]['@history'][i]['@value'])
                                    jdict[key]['@history'][i]['@value'] = self.Traditional2Simplified(
                                        jdict[key]['@history'][i]['@value'])
                                    jdict[key]['@history'][i]['@value'] = self.full2half(
                                        jdict[key]['@history'][i]['@value'])
                                    jdict[key]['@history'][i]['@value'] = self.nameCleaner(
                                        jdict[key]['@history'][i]['@value'])

                # 情况三：该字段有Value，且Value的类型为list
                if '@value' in jdict[key].keys() and type(jdict[key]['@value']) == list:
                    for i in range(len(jdict[key]['@value'])):
                        if type(jdict[key]['@value'][i]) == dict:
                            for key2 in jdict[key]['@value'][i].keys():
                                if type(jdict[key]['@value'][i][key2]) == unicode:
                                    if type(jdict[key]['@value'][i][key2].encode("utf-8")) == str:
                                        jdict[key]['@value'][i][key2] = self.symbolCleaner(
                                            jdict[key]['@value'][i][key2])
                                        jdict[key]['@value'][i][key2] = self.Traditional2Simplified(
                                            jdict[key]['@value'][i][key2])
                                        jdict[key]['@value'][i][key2] = self.full2half(
                                            jdict[key]['@value'][i][key2])
                                        jdict[key]['@value'][i][key2] = self.nameCleaner(
                                            jdict[key]['@value'][i][key2])

                # 情况四：该字段中有History，但是History中不存在'@value'字段，可能存在'@structValue'字段
                if '@history' in jdict[key].keys() and type(jdict[key]['@history']) == list:
                    for i in range(len(jdict[key]['@history'])):
                        if type(jdict[key]['@history'][i]) == dict:

                            if '@value' not in jdict[key]['@history'][i].keys():
                                for key2 in jdict[key]['@history'][i].keys():
                                    if type(jdict[key]['@history'][i][key2]) == unicode:
                                        if type(jdict[key]['@history'][i][key2].encode("utf-8")) == str:
                                            jdict[key]['@history'][i][key2] = self.symbolCleaner(
                                                jdict[key]['@history'][i][key2])
                                            jdict[key]['@history'][i][key2] = self.Traditional2Simplified(
                                                jdict[key]['@history'][i][key2])
                                            jdict[key]['@history'][i][key2] = self.full2half(
                                                jdict[key]['@history'][i][key2])
                                            jdict[key]['@history'][i][key2] = self.nameCleaner(
                                                jdict[key]['@history'][i][key2])

                            if '@structValue' in jdict[key]['@history'][i].keys():
                                if type(jdict[key]['@history'][i]['@structValue']) == dict:
                                    for key2 in jdict[key]['@history'][i]['@structValue'].keys():
                                        if type(jdict[key]['@history'][i]['@structValue'][key2]) == unicode:
                                            if type(jdict[key]['@history'][i]['@structValue'][key2].encode(
                                                    "utf-8")) == str:
                                                jdict[key]['@history'][i]['@structValue'][key2] = self.symbolCleaner(
                                                    jdict[key]['@history'][i]['@structValue'][key2])
                                                jdict[key]['@history'][i]['@structValue'][
                                                    key2] = self.Traditional2Simplified(
                                                    jdict[key]['@history'][i]['@structValue'][key2])
                                                jdict[key]['@history'][i]['@structValue'][key2] = self.full2half(
                                                    jdict[key]['@history'][i]['@structValue'][key2])
                                                jdict[key]['@history'][i]['@structValue'][key2] = self.nameCleaner(
                                                    jdict[key]['@history'][i]['@structValue'][key2])

                # 情况五：该字段中存在'@structValue'字段
                if '@structValue' in jdict[key].keys():
                    if type(jdict[key]['@structValue']) == dict:
                        for key2 in jdict[key]['@structValue'].keys():
                            if type(jdict[key]['@structValue'][key2]) == unicode:
                                if type(jdict[key]['@structValue'][key2].encode("utf-8")) == str:
                                    jdict[key]['@structValue'][key2] = self.symbolCleaner(
                                        jdict[key]['@structValue'][key2])
                                    jdict[key]['@structValue'][key2] = self.Traditional2Simplified(
                                        jdict[key]['@structValue'][key2])
                                    jdict[key]['@structValue'][key2] = self.full2half(
                                        jdict[key]['@structValue'][key2])
                                    jdict[key]['@structValue'][key2] = self.nameCleaner(
                                        jdict[key]['@structValue'][key2])




        # Step3: 处理时间类型及金额类型
        for key in list(jdict.keys()):
            if type(jdict[key]) == dict:
                # @崔鹏
                if key in self.pdict["date"]:
                    for i in range(len(jdict[key]["@history"])):
                        if "@value" in jdict[key]["@history"][i].keys():
                            jdict[key]["@history"][i]["@value"] = self.dateCleaner(jdict[key]["@history"][i]["@value"])
                    jdict[key]["@value"] = self.dateCleaner(jdict[key]["@value"])
                elif key in self.pdict["RegistCapi"]:
                    for i in range(len(jdict[key]["@history"])):
                        if "@value" in jdict[key]["@history"][i].keys():
                            jdict[key]["@history"][i]["@value"] = self.moneyCleaner(jdict[key]["@history"][i]["@value"])
                    jdict[key]["@value"] = self.moneyCleaner(jdict[key]["@value"])




        # Step4: 清洗营运状态（要在处理History之前），清洗公司名称
        for key in list(jdict.keys()):
            if key == 'P00000011':
                jdict[key]['@value'] = self.changeStatus(jdict[key]['@value'])
                for i in range(len(jdict[key]['@history'])):
                    jdict[key]['@history'][i]['@value'] = self.changeStatus(jdict[key]['@history'][i]['@value'])
            if key == 'P00000001':
                jdict[key]['@value'] = self.cleanCompanyName(jdict[key]['@value'])
                for i in range(len(jdict[key]['@history'])):
                    jdict[key]['@history'][i]['@value'] = self.cleanCompanyName(jdict[key]['@history'][i]['@value'])




        # Step5: 处理高管字段中的History重复情况，并更新其Value
        flag_all = 0
        for key in list(jdict.keys()):
            if key != 'P00000015' and key != 'P00000018':
                continue
            flag = 0
            i = 0
            # 进行一次双层循环，将相同的人名合并，并更改endTime
            if type(jdict[key]) == dict and '@history' in jdict[key].keys() and type(jdict[key]['@history']) == list:
                while i < (len(jdict[key]['@history'])):
                    try:
                        temp_dict_i = jdict[key]['@history'][i]
                        temp_dict_i_new = {key: val for key, val in temp_dict_i.items() if
                                           key != '@startTime' and key != '@endTime'}
                    except Exception as e:
                        break

                    for j in range(0, len(jdict[key]['@history'])):
                        if j == i:
                            continue
                        try:
                            temp_dict_j = jdict[key]['@history'][j]
                            temp_dict_j_new = {key: val for key, val in temp_dict_j.items() if
                                               key != '@startTime' and key != '@endTime'}
                        except Exception as e:
                            break

                        if temp_dict_i_new == temp_dict_j_new:
                            # 如果startTime和endTime存在
                            if '@startTime' in temp_dict_i.keys() and temp_dict_i[
                                '@startTime'] != "unknown" and '@endTime' in temp_dict_j.keys() and temp_dict_j[
                                '@endTime'] != "unknown":
                                # 如果时间差14天以内，那么合并人员
                                startTime = datetime.datetime.strptime(jdict[key]['@history'][i]['@startTime'],
                                                                       '%Y-%m-%d').date()
                                endTime = datetime.datetime.strptime(jdict[key]['@history'][j]['@endTime'],
                                                                     '%Y-%m-%d').date()
                                if startTime.__sub__(endTime).days <= 14:
                                    flag = 1
                                    flag_all = 1
                                    jdict[key]['@history'][j]['@startTime'] = "unknown"
                                    jdict[key]['@history'][j]['@endTime'] = "unknown"
                                    del jdict[key]['@history'][i]
                                    i = i - 1
                                    break

                            elif '@endTime' in temp_dict_i.keys() and temp_dict_i[
                                '@endTime'] != "unknown" and '@startTime' in temp_dict_j.keys() and temp_dict_j[
                                '@startTime'] != "unknown":
                                # 如果时间差三天以内，那么合并人员
                                startTime = datetime.datetime.strptime(jdict[key]['@history'][j]['@startTime'],
                                                                       '%Y-%m-%d').date()
                                endTime = datetime.datetime.strptime(jdict[key]['@history'][i]['@endTime'],
                                                                     '%Y-%m-%d').date()
                                if startTime.__sub__(endTime).days <= 14:
                                    flag = 1
                                    flag_all = 1
                                    jdict[key]['@history'][j]['@startTime'] = "unknown"
                                    jdict[key]['@history'][j]['@endTime'] = "unknown"
                                    del jdict[key]['@history'][i]
                                    i = i - 1
                                    break

                            # 特殊情况：如果History中的两个dict均相同，但是只有一个有时间的，就删去另一个。
                            elif '@startTime' in temp_dict_i.keys() and '@endTime' in temp_dict_i.keys() and \
                                    temp_dict_i['@startTime'] == "unknown" and temp_dict_i['@endTime'] == "unknown":
                                flag = 1
                                flag_all = 1
                                jdict[key]['@history'][j]['@startTime'] = "unknown"
                                jdict[key]['@history'][j]['@endTime'] = "unknown"
                                del jdict[key]['@history'][i]
                                i = i - 1
                                break

                            elif '@startTime' in temp_dict_j.keys() and '@endTime' in temp_dict_j.keys() and \
                                    temp_dict_j['@startTime'] == "unknown" and temp_dict_j['@endTime'] == "unknown":
                                flag = 1
                                flag_all = 1
                                jdict[key]['@history'][j]['@startTime'] = "unknown"
                                jdict[key]['@history'][j]['@endTime'] = "unknown"
                                del jdict[key]['@history'][i]
                                i = i - 1
                                break

                            # 如果不存在时间且完全一样，直接去重History
                            elif temp_dict_i == temp_dict_j:
                                flag = 1
                                flag_all = 1
                                del jdict[key]['@history'][i]
                                i = i - 1
                                break
                    i = i + 1

            # 处理History，如果Value的值为[], 则将History中的endTime均变为'unknown'
            if type(jdict[key]) == dict and '@history' in jdict[key].keys() and '@value' in jdict[key].keys():
                if jdict[key]['@value'] == []:
                    flag = 1
                    flag_all = 1
                    for i in range(len(jdict[key]['@history'])):
                        if '@endTime' in jdict[key]['@history'][i].keys():
                            jdict[key]['@history'][i]['@endTime'] = "unknown"


            # 如果flag大于0，说明该字段对应的数据为问题数据
            if flag > 0 and type(jdict[key]) == dict and '@history' in jdict[key].keys() and '@value' in jdict[
                key].keys():
                if type(jdict[key]['@value']) == str:
                    jdict[key]['@value'] = ""
                if type(jdict[key]['@value']) == list:
                    jdict[key]['@value'] = []

                for i in range(len(jdict[key]['@history'])):
                    # 如果History中存在Value的值，那么直接更新Value值和updateTime的值
                    if "@endTime" in jdict[key]['@history'][i].keys() and jdict[key]['@history'][i][
                        '@endTime'] == "unknown" and "@value" in jdict[key]['@history'][i].keys():
                        jdict[key]['@value'] = jdict[key]['@history'][i]['@value']
                        jdict[key]['@updateTime'] = jdict[key]['@history'][i]['@updateTime']

                    # 如果History中不存在Value的值，那么将新产生的字典加入到List中去
                    if "@endTime" in jdict[key]['@history'][i].keys() and jdict[key]['@history'][i][
                        '@endTime'] == "unknown" and "@value" not in jdict[key]['@history'][i].keys():
                        new_value = {key: val for key, val in jdict[key]['@history'][i].items() if
                                     key != '@startTime' and key != '@endTime'}
                        if new_value not in jdict[key]['@value']:
                            jdict[key]['@value'].append(new_value)



        # Step6: 去除html标签所在的node
        for key in list(jdict.keys()):
            if type(jdict[key]) == dict:
                for key1 in jdict[key].keys():
                    if type(jdict[key][key1]) == unicode and type(jdict[key][key1].encode("utf-8")) == str:
                        if "img" and "src" in jdict[key][key1]:
                            del jdict[key][key1]
                            break
                    elif type(jdict[key][key1]) == list:
                        for i in range(len(jdict[key][key1])):
                            if type(jdict[key][key1][i]) == dict:
                                for key2 in jdict[key][key1][i].keys():
                                    if type(jdict[key][key1][i][key2]) == unicode:
                                        if type(jdict[key][key1][i][key2].encode("utf-8")) == str:
                                            if "img" and "src" in jdict[key][key1][i][key2]:
                                                del jdict[key][key1][i][key2]
                                                break
            if type(jdict[key]) == unicode:
                if type(jdict[key].encode("utf-8")) == str:
                    if "img" and "src" in jdict[key]:
                        del jdict[key]
                        break


        # Step7:清洗国家类型、证件类型等枚举类型
        for key in list(jdict.keys()):
            if type(jdict[key]) == dict:
                if '@history' in jdict[key].keys():
                    for i in range(len(jdict[key]['@history'])):
                        for key2 in jdict[key]['@history'][i].keys():
                            if key2 == 'P00000022':
                                jdict[key]['@history'][i][key2] = self.changeP022(jdict[key]['@history'][i][key2])
                            if key2 == 'P00000023':
                                jdict[key]['@history'][i][key2] = self.changeP023(jdict[key]['@history'][i][key2])

                if '@value' in jdict[key].keys():
                    if type(jdict[key]['@value']) == list:
                        for i in range(len(jdict[key]['@value'])):
                            if type(jdict[key]['@value'][i]) == dict:
                                for key2 in jdict[key]['@value'][i].keys():
                                    if key2 == 'P00000022':
                                        jdict[key]['@value'][i][key2] = self.changeP022(jdict[key]['@value'][i][key2])
                                    if key2 == 'P00000023':
                                        jdict[key]['@value'][i][key2] = self.changeP023(jdict[key]['@value'][i][key2])


            if key == 'P00000003' and type(jdict[key]) == dict:
                if '@value' in jdict[key].keys():
                    jdict[key]['@value'] = self.changeP003(jdict[key]['@value'])
                if '@history' in jdict[key].keys():
                    for i in range(len(jdict[key]['@history'])):
                        for key2 in jdict[key]['@history'][i].keys():
                            if key2 == '@value':
                                jdict[key]['@history'][i][key2] = self.changeP003(jdict[key]['@history'][i][key2])


        # Step8: 清洗完全重复的History，见cleanRepeatHis.py, 路径：/data1/de_six/liuchang/checkRes/err/cleanKG_TEMP
        flag_all2 = 0
        for key in jdict.keys():
            flag2 = 0
            i = 0
            # 进行一次双层循环，将相同的人名合并，并更改endTime
            if type(jdict[key]) == dict and '@history' in jdict[key].keys() and type(jdict[key]['@history']) == list:
                # 这里怎么想个办法，先将History去重处理
                while i < (len(jdict[key]['@history'])):
                    try:
                        temp_dict_i = jdict[key]['@history'][i]
                    except Exception as e:
                        break
                    for j in range(0, len(jdict[key]['@history'])):
                        if j == i:
                            continue
                        try:
                            temp_dict_j = jdict[key]['@history'][j]
                        except Exception as e:
                            break
                        if temp_dict_i == temp_dict_j:
                        # 如果不存在时间且完全一样，直接去重History
                            flag2 = 1
                            flag_all2 = 1
                            del jdict[key]['@history'][i]
                            i = i - 1
                            break
                    i = i + 1
            # 如果flag大于0，说明该字段对应的数据为问题数据
            if flag2 > 0 and type(jdict[key]) == dict and '@history' in jdict[key].keys() and '@value' in jdict[
                key].keys():
                if type(jdict[key]['@value']) == str:
                    jdict[key]['@value'] = ""
                if type(jdict[key]['@value']) == list:
                    jdict[key]['@value'] = []

                for i in range(len(jdict[key]['@history'])):
                    # 如果History中存在Value的值，那么直接更新Value值和updateTime的值
                    if "@endTime" in jdict[key]['@history'][i].keys() and jdict[key]['@history'][i][
                        '@endTime'] == "unknown" and "@value" in jdict[key]['@history'][i].keys():

                        jdict[key]['@value'] = jdict[key]['@history'][i]['@value']
                        jdict[key]['@updateTime'] = jdict[key]['@history'][i]['@updateTime']

                    # 如果History中不存在Value的值，那么将新产生的字典加入到List中去
                    if "@endTime" in jdict[key]['@history'][i].keys() and jdict[key]['@history'][i][
                        '@endTime'] == "unknown" and "@value" not in jdict[key]['@history'][i].keys():
                        new_value = {key: val for key, val in jdict[key]['@history'][i].items() if
                                     key != '@startTime' and key != '@endTime'}

                        if new_value not in jdict[key]['@value']:
                            jdict[key]['@value'].append(new_value)

        # Step8: 清洗公司名称P01，P100里的括号，英文括号转中文括号, 去除公司名称前后的空格
        for key in jdict.keys():
            if key == 'P00000001':
                jdict[key]['@value'] = self.cleanCompanyName(jdict[key]['@value'])
                for i in range(len(jdict[key]['@history'])):
                    if type(jdict[key]['@history'][i]['@value']) == str:
                        jdict[key]['@history'][i]['@value'] = self.cleanCompanyName(jdict[key]['@history'][i]['@value'])
                        jdict[key]['@history'][i]['@value'] = self.nameCleaner(jdict[key]['@history'][i]['@value'])

            if key == 'P00000100':
                if '@value' in jdict[key].keys() and jdict[key]['@value'] != []:
                    for i in range(len(jdict[key]['@value'])):
                        jdict[key]['@value'][i] = self.cleanCompanyName(jdict[key]['@value'][i])
                        jdict[key]['@value'][i] = self.nameCleaner(jdict[key]['@value'][i])

            if key == 'P00000018':
                if type(jdict[key]) == dict and '@history' in jdict[key].keys():
                    for i in range(len(jdict[key]['@history'])):
                        if type(jdict[key]['@history'][i]) == dict and 'P00000019' in jdict[key]['@history'][i].keys():
                            jdict[key]['@history'][i]['P00000019'] = self.cleanCompanyName(jdict[key]['@history'][i]['P00000019'])
                if type(jdict[key]) == dict and '@value' in jdict[key].keys():
                    for i in range(len(jdict[key]['@value'])):
                        if type(jdict[key]['@value'][i]) == dict and 'P00000019' in jdict[key]['@value'][i].keys():
                            jdict[key]['@value'][i]['P00000019'] = self.cleanCompanyName(jdict[key]['@value'][i]['P00000019'])

        # Step9: 添加清洗P02字段的代码 04-14
        for key in list(jdict.keys()):
            if key == 'P00000002':
                jdict[key]['@value'] = self.cleanP02(jdict[key]['@value'])
                jdict[key]['@value'] = self.cleanP02Upper(jdict[key]['@value'])
                for i in range(len(jdict[key]['@history'])):
                    jdict[key]['@history'][i]['@value'] = self.cleanP02(jdict[key]['@history'][i]['@value'])
                    jdict[key]['@history'][i]['@value'] = self.cleanP02Upper(jdict[key]['@history'][i]['@value'])

        # Step10: 清洗注册码字段
        for key in list(jdict.keys()):
            if key == 'P00000016':
                jdict[key]['@value'] = self.cleanP016(jdict[key]['@value'])
                for i in range(len(jdict[key]['@history'])):
                    jdict[key]['@history'][i]['@value'] = self.cleanP016(jdict[key]['@history'][i]['@value'])
            if key == "P00000025":
                for i in range(len(jdict[key]["@value"])):
                    if type(jdict[key]["@value"][i]) == dict:
                        for key2 in jdict[key]["@value"][i].keys():
                            if key2 == "P00000016":
                                jdict[key]["@value"][i][key2] = self.cleanP016(jdict[key]["@value"][i][key2])

        # Step11: 清洗经营范围
        for key in list(jdict.keys()):
            if key == 'P00000013':
                for i in range(len(jdict["P00000013"]["@history"])):
                    if "@value" in jdict["P00000013"]["@history"][i].keys():
                        jdict["P00000013"]["@history"][i]["@value"] = self.cleanHTML2(jdict["P00000013"]["@history"][i]["@value"])
                if "@value" in jdict["P00000013"].keys():
                    jdict["P00000013"]["@value"] = self.cleanHTML2(jdict["P00000013"]["@value"])

        return jdict


    def cleanP016(self, check_str):
        count = 0
        for ch in check_str.decode('utf-8'):
            if u'\u4e00' <= ch <= u'\u9fff':
                count += 1
        if count == 0:
            return check_str
        else:
            return "NONE"

    # 统一信用代码英文是大写的
    def cleanP02Upper(self, str1):
        return str1.upper()

    def cleanP02(self, str1):
        flag = 0
        if str1 == "NONE":
            return str1
        if len(str1) < 18:
            flag += 1
        if '\u' in str1:
            flag += 1
        if '*' in str1:
            flag += 1
        if 'null' in str1:
            flag += 1
        if flag == 0:
            return str1
        else:
            return "NONE"


    #规范时间格式为YYYY-MM-DD
    def dateCleaner(self, data):
        pt1=u'[\d]{4}年[\d]{1,2}月[\d]{1,2}日'
        res = re.match(pt1,data)
        if res != None:
            ptn1 = u'[年月]'
            ptn2 = u'日'
            data = res.group(0)
            data = re.sub(ptn1, '-', data)
            data = re.sub(ptn2, '', data)
            return data

        return data


    # 检查时间格式为YYYY-MM-DD格式
    def dateCheck(self, data):
        if data == 'NONE':
            return 0
        pt1 = u'[\d]{4}-[\d]{1,2}-[\d]{1,2}'
        res = re.match(pt1, data)
        if res == None:
            return 1
        return 0


    #规范金额格式为xxxxx元或其他货币
    def moneyCleaner(self, data):
        if data == "NONE":
            return data
        if data == "unknow":
            return data
        #data=unicode(data,'utf8')
        if type(data) == int or type(data) == float:
            data = str(data)+u"万元"
        pt1 = r'[^\x00-\xff]'
        regex = re.compile(pt1)
        unit = regex.findall(data)

        if len(unit) == 0:
            unit = [u'元']
        try:
            num = re.search("\d+(\.\d+)?", data).group()
        except:
            return "unknow"
        num = float(num.encode("utf-8"))

        if u'万' in unit:
            num *= 10000
            num = str(num)
        if len(unit) > 1:
            if u'人' in unit:
                result = str(num) + u'元'
            elif u'万' in unit:
                result = str(num)
                for i in range(1, len(unit)):
                    result += unit[i]
            elif u'万' not in unit:
                result = str(num)
                for i in range(0, len(unit)):
                    result += unit[i]
        else:
            result = str(num) + u'元'
        return result


    # 全角转半角
    def full2half(self, s):
        table = {ord(f): ord(t) for f, t in zip(
            u'，。！？【】（）％＃＠＆１２３４５６７８９０',
            u',.!?[]()%#@&1234567890')}
        n = []
        s = s.decode('utf-8')
        for char in s:
            num = ord(char)
            if num == 0x3000:
                num = 32
            elif 0xFF01 <= num <= 0xFF5E:
                num -= 0xfee0
            num = unichr(num)
            n.append(num)
        str_temp = ''.join(n)
        return str_temp.translate(table)


    def Traditional2Simplified(self, sentence):
        sentence = Converter('zh-hans').convert(sentence)
        return sentence


    def nameCleaner(self, name):
        return name.strip()


    def changeStatus(self, str1):
        status1 = '存续'
        status1_a = '在营'
        status1_b = '开业'
        status1_c = '在册'
        status1_d = '在业'
        status1_e = '正常'
        status1_f = '登记成立'
        status1_g = '已开业'
        status1_h = '正常执业'
        status1_i = '正常在业'
        status1_j = '成立'
        status1_k = '仍注册'

        status2 = '吊销，未注销'
        status2_a = '吊销未注销'
        status2_b = '吊销,未注销'
        status2_c = '已吊销'
        status2_d = '吊销'
        status2_e = '吊销企业'
        status2_f = '吊销后未注销'

        status3 = '注销'
        status3_a = '已注销'
        status3_b = '注销企业'
        status3_c = '企业直接申请注销'
        status3_d = '吊销后注销'
        status3_e = '吊销并注销'
        status3_f = '注销(简易)'
        status3_g = '迁出注销'

        status4 = '其他'
        status5 = '解散'

        if status5 in str1:
            return status5

        if status1 in str1 or status1_a in str1 or  status1_b in str1 or  status1_c in str1 or  status1_d in str1:
            return status1
        if str1 == status1_e or str1 == status1_f or str1 == status1_g or str1 == status1_h:
            return status1
        if str1 == status1_i or str1 == status1_j or str1 == status1_k:
            return status1

        if status2 in str1 or status2_a in str1 or status2_b in str1:
            return status2
        if str1 == status2_c or str1 == status2_d or str1 == status2_e or str1 == status2_f:
            return status2

        if status3_a in str1 or status3_b in str1 or status3_c in str1:
            return status3
        if str1 == status3 or str1 == status3_d or str1 == status3_e or str1 == status3_f or str1 == status3_g:
            return status3

        else:
            return status4


    def symbolCleaner(self, str1):
        if '（' in str1:
            str1 = str1.replace('（', '(')
        if '）' in str1:
            str1 = str1.replace('）', ')')
        return str1

    def companyNameSymbolCleaner(self, str1):
        if '(' in str1:
            str1 = str1.replace('(', '（')
        if ')' in str1:
            str1 = str1.replace(')', '）')
        return str1

    def remove(self, str1):
        return "".join(str1.split())


    def removeSpecial(self, str1):
        res1 = str1.rstrip('*')
        res2 = res1.rstrip('`')
        return res2

    def removeRN(self, str1):
        str1 = str1.replace('\\r','')
        str1 = str1.replace('\\n','')
        return str1

    def changeP023(self, str1):
        res1 = "中国(香港)"
        res2 = "中国(澳门)"
        res3 = "中国(台湾)"
        res4 = "中国"
        if '香港' in str1 and '居住在' not in str1:
            return res1
        if '澳门' in str1 and '居住在' not in str1:
            return res2
        if '台湾' in str1 and '居住在' not in str1:
            return res3
        if str1 == '中 国':
            return res4
        return str1


    def changeP022(self, str1):
        res = "中华人民共和国居民身份证"
        if str1 == "居民身份证":
            return res
        return str1


    def changeP003(self, str1):
        try:
            number = int(str1[-5:-1])
            if number >= 1000 and number < 9999:
                str1 = str1[:-6]
        except:
            pass
        if str1 == "港、澳、台投资企业分支机构":
            str1 = "台、港、澳投资企业分支机构"
        elif str1 == "6180":
            str1 = "分公司"
        elif str1 == "一人有限责任公司(外商投资企业法人独资)":
            str1 = "一人有限责任公司"
        elif str1 == "股份有限公司(其他台港澳股份有限公司)":
            str1 = "股份有限公司"
        elif str1 == "内资":
            str1 = "内资公司"
        elif str1 == "股份有限公司(台港澳与境内合资、未上市)分支机构":
            str1 = "股份有限公司(台港澳与境内合资、未上市)"
        elif str1 == "股份合作制(非法人)":
            str1 = "股份合作制"
        elif str1 == "国有与集体企业联营":
            str1 = "联营"
        elif str1 == "外国(地区)企业在中国境内从事经营活动保险" or str1 == "外国(地区)企业在中国境内从事经营活动银行":
            str1 = "外国(地区)企业在中国境内从事经营活动"
        elif str1 == "非公司港、澳、台投资企业分支机构":
            str1 = "非公司台、港、澳投资企业分支机构"
        elif str1 == "外国(地区)有限责任公司分支机构分公司":
            str1 = "外国(地区)有限责任公司分支机构"
        elif str1 == "股份合作企业分支机构":
            str1 = "股份合作制"
        elif str1 == "有限责任公司(以投资为主要业务的外商投资合伙企业投资)分公司":
            str1 = "有限责任公司"
        elif str1 == "有限责任公司(以投资为主要业务的外商投资合伙企业投资)":
            str1 = "有限责任公司"
        elif str1 == "有限责任公司(外国法人独资)分支机构":
            str1 = "有限责任公司"
        elif str1 == "有限责公司分公司(国有独资)":
            str1 = "有限责任公司分公司(国有独资)"
        elif str1 == "股份制(全资设立)":
            str1 = "股份制"
        qlist = ["联营分支机构","外国(地区)企业分公司","股份公司投资","制造业","合伙制","全民与全民联营","中外合作企业分支机构",
                 "事业法人单位营业","其他外商投资股份有限公司","普通的合伙所","外商独资企业办事机构",
                 "普通中外合伙","科技类","个体(台、港、澳)","合伙所","会员制","来件装配","其它经济成份联营",
                 "独资私营企业","外资合分支机构","外商保险分公司","个人所","矿产资源勘探开发","非上市股份有限公司分公司",
                 "全民与集体联营","上市股份有限公司","有限责任公司(外商投资、非独资)分公司",
                 "有限责任公司分支机构(台、港、澳投资企业)","股份有限公司(外商合资、未上市)分公司","非公司港、澳、台企业(港澳台与境内合作)",
                 "内资集团","普通台港澳合伙","有限合伙企业(外商投资)","集体(内联-独资)","其他(相互保险社)","承包经营管理外商投资企业",
                 "银行","合资经营(台资)"]
        if str1 in qlist:
            str1 = "其他"
        return str1


    def cleanCompanyName(self, str1):
        str1 = self.full2half(str1) # 全角转半角
        str1 = self.Traditional2Simplified(str1) # 繁体转简体
        str1 = self.companyNameSymbolCleaner(str1) # 处理中英文括号
        str1 = self.nameCleaner(str1) # 处理前后的空格
        str1 = self.remove(str1) # 处理字符中的其他空格
        str1 = self.removeSpecial(str1) # 移除最后的特殊字符
        return str1

    def cleanPunish(self, str1):
        str1 = self.full2half(str1) # 全角转半角
        str1 = self.Traditional2Simplified(str1) # 繁体转简体
        str1 = self.symbolCleaner(str1) # 处理中英文括号
        str1 = self.nameCleaner(str1) # 处理前后的空格
        str1 = self.remove(str1) # 处理字符中的其他空格
        str1 = self.removeRN(str1) # 移除最后的特殊字符
        return str1

    def cleanHTML2(self, str1):
        str1 = re.sub(r'<[^>]+>', '', str1)
        return str1

    def cleanData2(self, jdict):
        for key in list(jdict.keys()):
            if type(jdict[key]) != dict and type(jdict[key]) != list:
                # Step1: 处理空值
                if jdict[key] == [] and key != 'P00000050' and key != 'P00000081' and key != 'P00000095' and key != 'P00000031':
                    del jdict[key]
                    continue
                if jdict[key] is None:
                    del jdict[key]
                    continue
                if jdict[key] == "":
                    jdict[key] = "NONE"
                if jdict[key] == " ":
                    jdict[key] = "NONE"
                # Step2: 处理中英文符号，全角转半角，繁体转简体，名字中的空格等
                if type(jdict[key]) == unicode and type(jdict[key].encode("utf-8")) == str:
                    jdict[key] = self.full2half(jdict[key])
                    jdict[key] = self.Traditional2Simplified(jdict[key])
                    jdict[key] = self.symbolCleaner(jdict[key])
                    jdict[key] = self.nameCleaner(jdict[key])
                    if "img" in jdict[key] and "src" in jdict[key]:
                        del jdict[key]
                        continue

                #Step3: 处理人名、公司名以及经营范围对应的空格
                namedict = ['P00000001', 'P00000004', 'P00000013', 'P00000015', 'P00000018', 'P00000019']
                if key in namedict:
                    if type(jdict[key]) == unicode and type(jdict[key].encode("utf-8")) == str:
                        jdict[key] = self.nameCleaner(jdict[key])
                #Step4: 处理时间类型
                if key in self.pdict["date"]:
                    jdict[key] = self.dateCleaner(jdict[key])
                    res = self.dateCheck(jdict[key])
                    if res == 1:
                        del jdict[key]
                        continue
                #Step5: 处理金额类型
                if key in self.pdict["RegistCapi"]:
                    jdict[key] = self.moneyCleaner(jdict[key])
                #Step6: 处理营运状态
                if key == 'P00000011':
                    jdict[key] = self.changeStatus(jdict[key])
                #Step6: 处理经营范围
                if key == 'P00000013':
                    jdict[key] = self.cleanHTML2(jdict[key])
                #Step7: 处理P003
                if key == 'P00000003':
                    jdict[key] = self.changeP003(jdict[key])
                #Step8: 处理公司名称
                if key == 'P00000001' or key == 'P00000018':
                    jdict[key] = self.cleanCompanyName(jdict[key])
                # Step9: 处理P02字段
                if key == 'P00000002':
                    jdict[key] = self.cleanP02(jdict[key])
                    jdict[key] = self.cleanP02Upper(jdict[key])

                if key == "P00000016":
                    jdict[key] = self.cleanP016(jdict[key])

            if type(jdict[key]) == list:
                if jdict[key] == [] and key != 'P00000050' and key != 'P00000081' and key != 'P00000095' and key != 'P00000031':
                    del jdict[key]
                    continue
                for i in range(len(jdict[key])):
                    if type(jdict[key][i]) == dict:
                        for key2 in list(jdict[key][i].keys()):
                            if jdict[key][i][key2] == []:
                                del jdict[key][i][key2]
                                if jdict[key][i] == {}:
                                    del jdict[key][i]
                                    if jdict[key] == []:
                                        del jdict[key]
                                continue
                            if jdict[key][i][key2] == "":
                                jdict[key][i][key2] = "NONE"
                            if jdict[key][i][key2] == " ":
                                jdict[key][i][key2] = "NONE"
                            if jdict[key][i][key2] == "None":
                                jdict[key][i][key2] = "NONE"
                            if jdict[key][i][key2] == "null":
                                jdict[key][i][key2] = "NONE"
                            if jdict[key][i][key2] is None:
                                del jdict[key][i][key2]
                                if jdict[key][i] == {}:
                                    del jdict[key][i]
                                    if jdict[key] == []:
                                        del jdict[key]
                                continue
                            if  type(jdict[key][i][key2]) == unicode:
                                if type(jdict[key][i][key2].encode("utf-8")) == str:
                                    if self.remove(jdict[key][i][key2]) == "":
                                        jdict[key][i][key2] = "NONE"
                                    jdict[key][i][key2] = self.full2half(jdict[key][i][key2])
                                    jdict[key][i][key2] = self.Traditional2Simplified(jdict[key][i][key2])
                                    jdict[key][i][key2] = self.symbolCleaner(jdict[key][i][key2])
                                    jdict[key][i][key2] = self.nameCleaner(jdict[key][i][key2])
                                    if "img" in jdict[key][i][key2] and "src" in jdict[key][i][key2]:
                                        del jdict[key][i][key2]
                                        continue


                            namedict = ['P00000001', 'P00000004', 'P00000013', 'P00000015', 'P00000018', 'P00000019']
                            if key2 in namedict:
                                if type(jdict[key][i][key2]) == unicode and type(jdict[key][i][key2].encode("utf-8")) == str:
                                    jdict[key][i][key2] = self.nameCleaner(jdict[key][i][key2])
                            # Step4: 处理时间类型
                            if key2 in self.pdict["date"]:
                                jdict[key][i][key2] = self.dateCleaner(jdict[key][i][key2])
                            # Step5: 处理金额类型
                            if key2 in self.pdict["RegistCapi"]:
                                jdict[key][i][key2] = self.moneyCleaner(jdict[key][i][key2])
                            # Step6: 处理营运状态
                            if key2 == 'P00000011':
                                jdict[key][i][key2] = self.changeStatus(jdict[key][i][key2])
                            if key2 == 'P00000022':
                                jdict[key][i][key2] = self.changeP022(jdict[key][i][key2])
                            if key2 == 'P00000023':
                                jdict[key][i][key2] = self.changeP023(jdict[key][i][key2])
                            # 清洗公司名称
                            if key2 == 'P00000001' or key2 == 'P00000019':
                                jdict[key][i][key2] = self.cleanCompanyName(jdict[key][i][key2])
                            if key2 == 'P00000057':
                                jdict[key][i][key2] = self.cleanPunish(jdict[key][i][key2])
                            if key2 == 'P00000002':
                                jdict[key][i][key2] = self.cleanP02(jdict[key][i][key2])
                                jdict[key][i][key2] = self.cleanP02Upper(jdict[key][i][key2])
                            if key2 == 'P00000016':
                                jdict[key][i][key2] = self.cleanP016(jdict[key][i][key2])

        return jdict


if __name__ == "__main__":
    oc = objectClean("schema_clean.conf")
    for line in sys.stdin:
        jdict = json.loads(line[:-1].split("\t")[1])
        oc.cleanData2(jdict)
        print(line[:-1].split("\t")[0] + '\t' + json.dumps(jdict, ensure_ascii=False))


