#!/usr/bin/env python
# encoding: utf-8
'''
@author: liuchang
@software: PyCharm
@file: oldest.py
@time: 2020-04-22 10:55
'''
import sys

time = ""
for line in sys.stdin:
    if "BAK" not in line[:-1]:
        continue
    if "KG-basic" not in line[:-1].split(".")[0]:
        continue
    t = " ".join(line[:-1].split(".")[-2:])
    #sys.stderr.write(t+"\n")
    if time == "":
        time = t
    if t < time and time != "" and t != "":
        time = t
print(time)
