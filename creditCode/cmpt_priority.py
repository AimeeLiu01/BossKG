#!/usr/bin/env python
# encoding: utf-8
'''
@author: liuchang
@software: PyCharm
@file: cmpt_priority.py
@time: 2020-06-20 09:37
'''
# -*- coding: utf-8 -*-
import json
import os
import sys
import re
import math
import datetime
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('./')


if __name__ == "__main__":
    for line in sys.stdin:
#        order = int(line[:-1].split("\t")[5])
       # if order >= 4916:
#        if order >= 4706 and order <= 4915:
#            print line[:-1].split("\t")[1] + "\t" + line[:-1].split("\t")[3] + "\t" + line[:-1].split("\t")[2] + "\t" + str(order)
        order = int(line[:-1].split("\t")[5])
        neworder = 4936 - order
        print(line[:-1].split("\t")[0] + "\t" + line[:-1].split("\t")[1] + "\t" + line[:-1].split("\t")[2] + "\t" + line[:-1].split("\t")[3] + "\t" + line[:-1].split("\t")[4] + "\t" + str(neworder))
