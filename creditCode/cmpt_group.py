#!/usr/bin/env python
# encoding: utf-8
'''
@author: liuchang
@software: PyCharm
@file: cmpt_group.py.py
@time: 2020-06-20 09:38
'''
# -*- coding: utf-8 -*-
import json
import os
import sys
import re
import math
import datetime
import time
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('./')


if __name__ == "__main__":
    #ob = objectClean()
    first = "2020-06-01"
    today = datetime.date.today().strftime('%Y-%m-%d')
    today = datetime.datetime.strptime(today,"%Y-%m-%d")
    firstday = datetime.datetime.strptime(first,'%Y-%m-%d')
    num = (today - firstday).days
#    print num
    for line in sys.stdin:
        if 'A' in line[:-1].split("\t")[4]:
            print(line[:-1])
        if 'B' in line[:-1].split("\t")[4]:
            num1 = num % 7
            num2 = 'B'+str(num1)
            if num1 == 0:
                num2 = "B7"
            if line[:-1].split("\t")[4] == num2:
                print(line[:-1])
        if 'C' in line[:-1].split("\t")[4]:
            num1 = num % 14
            num2 = 'C'+str(num1)
            if num1 == 0:
                num2 = "C14"
            if line[:-1].split("\t")[4] == num2:
                print(line[:-1])
        if 'D' in line[:-1].split("\t")[4]:
            num1 = num % 60
            num2 = 'D'+str(num1)
            if num1 == 0:
                num2 = "D60"
            if line[:-1].split("\t")[4] == num2:
                print(line[:-1])

