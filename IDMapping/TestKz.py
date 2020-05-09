#!/usr/bin/env python
# encoding: utf-8
'''
@author: liuchang
@software: PyCharm
@file: TestKz.py
@time: 2020-05-09 09:27
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


def mkKZid(x):
    output = []
    kgid = "-"
    if "@kg_id" in x.keys():
        kgid = x["@kg_id"]
    kzid = "-"
    if "P00000014" in x and x["P00000014"]["@value"] != []:
        for item in x["P00000014"]["@value"]:
            kzid = item['id']
            output.append((kzid, kgid))
    return output


def kgKZid(x):
    kg_id = "-"
    P014 = "-"
    jdict = {}
    output = []
    if "@kg_id" in x.keys():
        kg_id = x["@kg_id"]
    if "P00000014" in x.keys():
        P014 = x["P00000014"]
        jdict["P00000014"] = P014
        return kg_id + "\t" + json.dumps(jdict, ensure_ascii=False)
    return ""


def mkP14NewData(x):
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
            output.append((item, (kgid, status, score, endTime, zhuceTime, p02)))
        return output

def prepareP14Data(x, y, z):
    kg_id = y
    kz_id = x
    name = z
    today = datetime.now().strftime('%Y-%m-%d')
    jdict = {}
    key = 'P00000014'
    value_dict = {}
    value_dict["@source"] = "cmpt-IDMapping"
    value_dict["@updateTime"] = today
    value_dict["@kg_id"] = kg_id
    value_dict["@type"] = "organization"
    value_dict["@value"] = []
    dic = {}
    dic = {'id': kz_id, 'name': name}
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
    x_time = x_data["P00000014"]["@updateTime"]
    y_time = y_data["P00000014"]["@updateTime"]
    x_value = x_data["P00000014"]["@value"]
    y_value = y_data["P00000014"]["@value"]
    if x_time == today:
        for i in range(len(x_data["P00000014"]["@value"])):
            if x_data["P00000014"]["@value"][i] not in y_data["P00000014"]["@value"]:
                return x
        return y
    if y_time == today:
        for i in range(len(y_data["P00000014"]["@value"])):
            if y_data["P00000014"]["@value"][i] not in x_data["P00000014"]["@value"]:
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
    x_time = x_data["P00000014"]["@updateTime"]
    y_time = y_data["P00000014"]["@updateTime"]
    if x_time == today:
        for i in range(len(y_data["P00000014"]["@value"])):
            if y_data["P00000014"]["@value"][i] not in x_data["P00000014"]["@value"]:
                x_data["P00000014"]["@value"].append(y_data["P00000014"]["@value"][i])
        # print "unionID 1..."
        # print  json.dumps(x_data)
        return (json.dumps(x_data, ensure_ascii=False) ,'1')
    if y_time == today:
        for i in range(len(x_data["P00000014"]["@value"])):
            if x_data["P00000014"]["@value"][i] not in y_data["P00000014"]["@value"]:
                y_data["P00000014"]["@value"].append(x_data["P00000014"]["@value"][i])
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


sc = SparkContext(appName='IDMappingKZ')
hive_context= HiveContext(sc)

#----------------------KZ Mapping-------------------------#
# Step2: 处理KZ数据库
hive_database = "ods_kanzhun_company"
hive_table = "kanzhun_company"
hive_read = "select id, full_name from {}.{}".format(hive_database, hive_table)


# 1. KG-basic库中的(kzid, kgid)
kg_kzid = sc.textFile(input_path)
kg_kzid = kg_kzid.map(lambda r : r.split('\t')[1]).map(json.loads).flatMap(lambda x : mkKZid(x)).filter(lambda x: x[1] != '-')

# 2. KZ数据库中的(kzid, name)
kz_data = hive_context.sql(hive_read).fillna('NONE').rdd.map(lambda x : (str(x.id), norm(x.full_name))).filter(lambda x: x[1] != '')

# 找出已经关联上的kzid, 并整理成kzid, kgid, name, 新的存量数据
kz_join_A = kg_kzid.leftOuterJoin(kz_data).filter(lambda x : x[1][1] != None)
kz_join_A = kz_join_A.filter(lambda x: len(x) == 2).filter(lambda x: x[1] != None).map(lambda x : prepareP14Data(x[0], x[1][0], x[1][1])).distinct()
kz_join_A = kz_join_A.map(lambda x: (x.split("\t")[0], (x.split("\t")[1],'1')))
kz_join_A = kz_join_A.reduceByKey(lambda x,y: unionID(x,y)).filter(lambda x: x != [])
# print 'KZ新的存量'
# print kz_join_A.count()
# print kz_join_A.take(5)

# 找出旧的存量数据.
kg_kzid2 = sc.textFile(input_path)
kg_kzid2 = kg_kzid2.map(lambda r : r.split('\t')[1]).map(json.loads).map(lambda x : kgKZid(x)).filter(lambda x: x != "").filter(lambda x: x.split("\t")[0] != "-").distinct()
kg_kzid2 = kg_kzid2.map(lambda x: (x.split("\t")[0], (x.split("\t")[1],'1')))
# print 'KZ旧的存量'
# print kg_kzid2.count()
# print kg_kzid2.take(5)

# 旧的存量数据和新的存量数据union，之后再ReduceByKey,生成新的存量数据（更新数据值）
# print 'KZ存量union'
cl_data_all = kz_join_A.union(kg_kzid2)
# print cl_data_all.count()
# print cl_data_all.take(5)

cl_data_all_reduce = cl_data_all.reduceByKey(lambda x,y:procedcl(x,y)).filter(lambda x: x != [])
# print 'KZ存量reduce，更新存量里的kzname'
# print cl_data_all_reduce.count()
# print cl_data_all_reduce.take(5)
cl_data_all_reduce.distinct().map(lambda x: x[0] + "\t" + x[1][0]).saveAsTextFile(output_path + '/P14CLData.'+ str(today))

# 3. left join找出没有匹配的kzid,[('7312118', (u'上海影视动画有限公司', None))]
kz_notjoin_B = kz_data.leftOuterJoin(kg_kzid).filter(lambda x : x[1][1] == None)

# 4. 整理kz库中待匹配的数据为(name, kzid)
kz_notjoin_B = kz_notjoin_B.map(lambda x: (x[1][0], x[0]))
# print '待匹配kzid'
# print kz_notjoin_B.take(5)

# 5. kg_data的数据格式为(name,kgid)
kg_data2 = sc.textFile(input_path)
kg_data2 = kg_data2.map(lambda r : r.split('\t')[1]).map(json.loads).flatMap(lambda x : mkP14NewData(x))

# 6.将没有分配出去的名字，关联上去，分配kzid
kz_data2 = kz_notjoin_B.leftOuterJoin(kg_data2).filter(lambda x : x[1][1] != None).distinct()
kz_2reduce = kz_data2.map(lambda x : (x[1][0], (x[0], x[1][1][0],x[1][1][1], x[1][1][2], x[1][1][3], x[1][1][4], x[1][1][5])))
# print "将kzid分配出去"
# print kz_2reduce.count()
# print kz_2reduce.take(5)
kz_2reduce.filter(lambda x: len(x) == 2).filter(lambda x: x[1] != None).map(lambda x : prepareP14Data(x[0], x[1][1], x[1][0])).distinct().saveAsTextFile(output_path + '/P14ZLData.' + str(today))

# 合并Before以及今天新加入的数据
# 读取之前的结果
kg_kzid = sc.textFile(output_path + '/P14CLData.' + str(today))
kg_kzid = kg_kzid.map(lambda x: (x.split("\t")[0], (x.split("\t")[1],'1')))
# print "KZ今日份存量数据"
# print kg_kzid.count()
# print kg_kzid.take(5)

# 读取今天产生的结果
add_kzid = sc.textFile(output_path + '/P14ZLData.' + str(today)).map(lambda x: (x.split("\t")[0], (x.split("\t")[1],'1')))
# print "KZ今日份增量数据"
# print add_kzid.count()
# print add_kzid.take(5)


all_kzid = add_kzid.union(kg_kzid)
# print "KZ今日份合并数据"
# print all_kzid.count()
# print all_kzid.take(5)

all_kzid2 = all_kzid.reduceByKey(lambda x,y: unionID(x,y)).filter(lambda x: x != [])
# print "KZ今日份Reduce数据(Append)"
# print all_kzid2.count()
# print all_kzid2.take(5)
today = datetime.now().strftime('%Y-%m-%d')
all_kzid2.map(lambda x: x[0] + "\t" + x[1][0]).filter(lambda x: today in x).saveAsTextFile(output_path + '/P14MappingRes.' + str(today))


# print "KZ Mapping End..."
sc.stop()
sys.stderr.write("^^^^^^^^^^^^^")


