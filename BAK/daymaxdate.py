#!/usr/bin/env python
# encoding: utf-8
'''
@author: liuchang
@software: PyCharm
@file: daymaxdate.py
@time: 2020-04-22 10:54
'''
import sys
import json
dict_t = {}
for line in sys.stdin:
    if "BAK" not in line[:-1]:
        continue
    if "KG-basic" not in line[:-1].split(".")[0]:
        continue
    t = " ".join(line[:-1].split(".")[-2:])
    date = t[0:10]
    time_h = t[-2:]
    if date not in dict_t.keys():
        dict_t[date] = time_h
    else:
        if int(time_h) > int(dict_t[date]):
            dict_t[date] = time_h
print(json.dumps(dict_t, ensure_ascii=False))


