#!/usr/bin/env python
# encoding: utf-8
'''
@author: liuchang
@software: PyCharm
@file: max_hour.py
@time: 2020-04-22 10:55
'''
import sys
import json
temp = ""
for line in sys.stdin:
    temp += line[:-1]
time_dict = json.loads(temp)
time_date = sys.argv[1]
print(time_dict[time_date])
