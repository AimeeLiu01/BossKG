#!/usr/bin/env python
# encoding: utf-8
'''
@author: liuchang
@software: PyCharm
@file: TestCRM.py
@time: 2020-05-09 09:47
'''
#-*- coding: UTF-8 -*-
import json
import re
import os
import sys
import chardet
import datetime
from datetime import datetime
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql import HiveContext
from pyspark import SparkContext
from pyspark.sql import Row

input_path = sys.argv[1]
output_path = sys.argv[2]
today = datetime.now().strftime('%Y-%m-%d')

def norm(x):
    return x


def mkCRMid(x):
    output = []
    kgid = "-"
    if "@kg_id" in x.keys():
        kgid = x["@kg_id"]
    crmid = "-"
    if "P00000094" in x and x["P00000094"]["@value"] != []:
        for item in x["P00000094"]["@value"]:
            crmid = item['id']
            output.append((crmid, kgid))
    return output

def kgCRMid(x):
    kg_id = "-"
    P094 = "-"
    jdict = {}
    output = []
    if "@kg_id" in x.keys():
        kg_id = x["@kg_id"]
    if "P00000094" in x.keys():
        P094 = x["P00000094"]
        jdict["P00000094"] = P094
        return kg_id + "\t" + json.dumps(jdict, ensure_ascii=False)
    return ""


def mkP94NewData(x):
    output = []
    score = '0'
    status = "-"
    endTime = "-"
    zhuceTime = "-"
    p02 = "-"
    if "P00000011" in x:
        status = x["P00000011"]["@value"].encode('utf8')
    kgid = "None"
    if "@kg_id" in x.keys():
        kgid = x["@kg_id"]
    if "P00000006" in x:
        zhuceTime = x["P00000006"]["@value"]
    if "P00000002" in x:
        p02 = x["P00000002"]["@value"]
    if "P00000001" in x and "@history" in x["P00000001"]:
        for item in x["P00000001"]["@history"]:
            if item["@endTime"] == "unknown":
                score = '1'
                endTime = item["@endTime"]
                output.append((item["@value"],(kgid,status,score,endTime,zhuceTime,p02)))
            if item["@endTime"] != "unknown":
                score = '2'
                endTime = item["@endTime"]
                output.append((item["@value"],(kgid,status,score,endTime,zhuceTime,p02)))
    if "P00000100" in x and x["P00000100"]["@value"] != []:
        for item in x["P00000100"]["@value"]:
            score = '2'
            endTime = 'P100'
            output.append((item,(kgid,status,score,endTime,zhuceTime,p02)))
    return output



def prepareP94Data(x, y, z):
    kg_id = y
    crm_id = x
    name = z
    today = datetime.now().strftime('%Y-%m-%d')
    jdict = {}
    key = 'P00000094'
    value_dict = {}
    value_dict["@source"] = "cmpt-IDMapping"
    value_dict["@updateTime"] = today
    value_dict["@kg_id"] = kg_id
    value_dict["@type"] = "organization"
    value_dict["@value"] = []
    dic = {}
    dic = {'id': crm_id, 'name': name}
    value_dict["@value"].append(dic)
    jdict[key] = value_dict
    return kg_id + '\t' + json.dumps(jdict, ensure_ascii=False)



def dateCheck(data):
    pt1 = u'[\d]{4}-[\d]{1,2}-[\d]{1,2}'
    res = re.match(pt1, data)
    if res == None:
        return 1
    return 0


def procedcl(x, y):
    if x == None and y != None:
       return y
    if y == None and x != None:
       return x
    if x == None and y == None:
       return []
    today = datetime.now().strftime('%Y-%m-%d')
    x_data = json.loads(x[0])
    y_data = json.loads(y[0])
    x_time = x_data["P00000094"]["@updateTime"]
    y_time = y_data["P00000094"]["@updateTime"]
    x_value = x_data["P00000094"]["@value"]
    y_value = y_data["P00000094"]["@value"]
    if x_time == today:
        for i in range(len(x_data["P00000094"]["@value"])):
            if x_data["P00000094"]["@value"][i] not in y_data["P00000094"]["@value"]:
                return x
        return y
    if y_time == today:
        for i in range(len(y_data["P00000094"]["@value"])):
            if y_data["P00000094"]["@value"][i] not in x_data["P00000094"]["@value"]:
                return y
        return x
    return x

def unionID(x,y):
    #print x
    #print y
    if x == None:
       return y
    if y == None:
       return x
    if x == None and y == None:
       return []
    if x == y:
       return x
    today = datetime.now().strftime('%Y-%m-%d')
    x_data = json.loads(x[0])
    y_data = json.loads(y[0])
    # print type(x_data)
    x_time = x_data["P00000094"]["@updateTime"]
    y_time = y_data["P00000094"]["@updateTime"]
    if x_time == today:
        for i in range(len(y_data["P00000094"]["@value"])):
            if y_data["P00000094"]["@value"][i] not in x_data["P00000094"]["@value"]:
                # print "!!!!!!!!!!!!!!!!!!!!!!!!!!"
                # print x_data["P00000094"]["@value"]
                # print y_data["P00000094"]["@value"][i]
                x_data["P00000094"]["@value"].append(y_data["P00000094"]["@value"][i])
                # print x_data["P00000094"]["@value"]
        # print "unionID 1..."
        # print  json.dumps(x_data)
        return (json.dumps(x_data, ensure_ascii=False) ,'1')
    if y_time == today:
        for i in range(len(x_data["P00000094"]["@value"])):
            if x_data["P00000094"]["@value"][i] not in y_data["P00000094"]["@value"]:
                y_data["P00000094"]["@value"].append(x_data["P00000094"]["@value"][i])
        # print "unionID 2..."
        # print json.dumps(y_data)
        return (json.dumps(y_data, ensure_ascii=False),'1')
    return x

def proced2(x,y):
    if x == None and y == None:
       return []
    if y == None and x != None:
       return x
    if x == None and y != None:
       return y
    if x[2] == "注销" and y[2] == "存续":
        return y
    if x[2] == "存续" and y[2] == "注销":
        return x
    if x[2] == "吊销，未注销" and y[2] == "存续":
        return y
    if x[2] == "存续" and y[2] == "吊销，未注销":
        return x
    if x[2] == "-" and y[2] == "存续":
        return y
    if x[2] == "存续" and y[2] == "-":
        return x
    # 如果一个是曾用名、一个是现用名、返回现用名
    if x[3] == '1' and y[3] == '2':
        return x
    if x[3] == '2' and y[3] == '1':
        return y
    # 如果两个都是曾用名，比较时间早晚，返回较大的时间
    if x[3] == '2' and y[3] == '2' and dateCheck(x[4]) == 0 and dateCheck(y[4]) == 0:
        endTime1 = datetime.strptime(x[4][:10], "%Y-%m-%d")
        endTime2 = datetime.strptime(y[4][:10], "%Y-%m-%d")
        if endTime1 > endTime2:
            return x
        if endTime1 < endTime2:
            return y
    # 如果两个注册时间都存在，比较时间，返回较大的时间
    if dateCheck(x[5]) == 0 and dateCheck(y[5]) == 0:
        zhuceTime1 = datetime.strptime(x[5][:10], "%Y-%m-%d")
        zhuceTime2 = datetime.strptime(y[5][:10], "%Y-%m-%d")
        if zhuceTime1 > zhuceTime2:
            return x
        if zhuceTime1 < zhuceTime2:
            return y
    # 如果一个注册时间存在，另一个不存在，返回注册时间存在的
    if dateCheck(x[5]) == 0 and dateCheck(y[5]) != 0:
        return x
    if dateCheck(x[5]) != 0 and dateCheck(y[5]) == 0:
        return y
    # 最后比较工商注册码，返回工商注册码长度为18的
    if len(x[6]) == 18 and len(y[6]) != 18:
        return x
    elif len(x[6]) != 18 and len(y[6]) == 18:
        return y
    else:
        #print 'CRM Marked!!!'
        #print x
        #print y
        return x
    return x


sc = SparkContext(appName='IDMappingCRM')
hive_context= HiveContext(sc)


#----------------------CRM Mapping-------------------------#
# Step2: 处理CRM数据库
hive_database = "ods_boss_crm"
hive_table = "customer"
hive_read = "select id, name from {}.{}".format(hive_database, hive_table)

# 1. KG-basic库中的(crmid, kgid)
kg_crmid = sc.textFile(input_path)
kg_crmid = kg_crmid.map(lambda r : r.split('\t')[1]).map(json.loads).flatMap(lambda x : mkCRMid(x)).filter(lambda x: x[1] != '-')

# 2. CRM数据库中的(crmid, name)
crm_data = hive_context.sql(hive_read).fillna('NONE').rdd.map(lambda x : (str(x.id), norm(x.name))).filter(lambda x: x[1] != '')

# 找出已经关联上的crmid, 并整理成crmid, kgid, name, 新的存量数据
crm_join_A = kg_crmid.leftOuterJoin(crm_data).filter(lambda x : x[1][1] != None)
crm_join_A = crm_join_A.filter(lambda x: len(x) == 2).filter(lambda x: x[1] != None).map(lambda x : prepareP94Data(x[0], x[1][0], x[1][1])).distinct()

crm_join_A = crm_join_A.map(lambda x: (x.split("\t")[0], (x.split("\t")[1],'1')))
crm_join_A = crm_join_A.reduceByKey(lambda x,y: unionID(x,y)).filter(lambda x: x != [])
# print '新的存量'
# print crm_join_A.count()
# print crm_join_A.take(5)

# 找出旧的存量数据.
kg_crmid2 = sc.textFile(input_path)
kg_crmid2 = kg_crmid2.map(lambda r : r.split('\t')[1]).map(json.loads).map(lambda x : kgCRMid(x)).filter(lambda x: x != "").filter(lambda x: x.split("\t")[0] != "-").distinct()
kg_crmid2 = kg_crmid2.map(lambda x: (x.split("\t")[0], (x.split("\t")[1],'1')))
# print '旧的存量'
# print kg_crmid2.count()
# print kg_crmid2.take(5)

# 旧的存量数据和新的存量数据union，之后再ReduceByKey,生成新的存量数据（更新数据值）
# print '存量union'
cl_data_all = crm_join_A.union(kg_crmid2)
# print cl_data_all.count()
# print cl_data_all.take(5)

cl_data_all_reduce = cl_data_all.reduceByKey(lambda x,y:procedcl(x,y)).filter(lambda x: x != [])
# print '存量reduce，更新存量里的crmname'
# print cl_data_all_reduce.count()
# print cl_data_all_reduce.take(5)
cl_data_all_reduce.distinct().map(lambda x: x[0] + "\t" + x[1][0]).saveAsTextFile(output_path + '/P94CLData.'+ str(today))


# 3. left join找出没有匹配的crmid,[('7312118', (u'上海影视动画有限公司', None))]
crm_notjoin_B = crm_data.leftOuterJoin(kg_crmid).filter(lambda x : x[1][1] == None)

# 4. 整理crm库中待匹配的数据为(name, crmid)
crm_notjoin_B = crm_notjoin_B.map(lambda x: (x[1][0], x[0]))
# print '待匹配crmid'
# print crm_notjoin_B.take(5)

# 5. kg_data的数据格式为(name,kgid)
kg_data2 = sc.textFile(input_path)
kg_data2 = kg_data2.map(lambda r : r.split('\t')[1]).map(json.loads).flatMap(lambda x : mkP94NewData(x))

# 6.将没有分配出去的名字，关联上去，分配crmid
crm_data2 = crm_notjoin_B.leftOuterJoin(kg_data2).filter(lambda x : x[1][1] != None).distinct()
crm_2reduce = crm_data2.map(lambda x : (x[1][0], (x[0], x[1][1][0],x[1][1][1], x[1][1][2], x[1][1][3], x[1][1][4], x[1][1][5])))
# print "将crmid分配出去"
# print crm_2reduce.count()
# print crm_2reduce.take(5)
#crm_2reduce.distinct().saveAsTextFile(output_path + '/CRM2Reduce.'+ str(today))

# print "crmid一对多个kgid,选其中一个"
reduce_res_crm = crm_2reduce.reduceByKey(lambda x,y:proced2(x,y)).filter(lambda x: x != [])
# print reduce_res_crm.count()
# print reduce_res_crm.take(5)
reduce_res_crm = reduce_res_crm.filter(lambda x: len(x) == 2).filter(lambda x: x[1] != None).map(lambda x : prepareP94Data(x[0], x[1][1], x[1][0])).distinct().saveAsTextFile(output_path + '/P94ZLData.' + str(today))

# 合并Before以及今天新加入的数据
# 读取之前的结果
kg_crmid = sc.textFile(output_path + '/P94CLData.' + str(today))
kg_crmid = kg_crmid.map(lambda x: (x.split("\t")[0], (x.split("\t")[1],'1')))
# print "今日份存量数据"
# print kg_crmid.count()
# print kg_crmid.take(5)

# 读取今天产生的结果
add_crmid = sc.textFile(output_path + '/P94ZLData.' + str(today)).map(lambda x: (x.split("\t")[0], (x.split("\t")[1],'1')))
# print "今日份增量数据"
# print add_crmid.count()
# print add_crmid.take(5)


all_crmid = add_crmid.union(kg_crmid)
# print "今日份合并数据"
# print all_crmid.count()
# print all_crmid.take(5)

all_crmid2 = all_crmid.reduceByKey(lambda x,y: unionID(x,y)).filter(lambda x: x != [])
# print "今日份Reduce数据"
# print all_crmid2.count()
# print all_crmid2.take(5)
today = datetime.now().strftime('%Y-%m-%d')
all_crmid2.map(lambda x: x[0] + "\t" + x[1][0]).filter(lambda x: today in x).saveAsTextFile(output_path + '/P94MappingRes.' + str(today))


# print "CRM Mapping End..."
sc.stop()
sys.stderr.write("^^^^^^^^^^^^^")