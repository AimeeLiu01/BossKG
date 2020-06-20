#!/usr/bin/env python
# encoding: utf-8
'''
@author: liuchang
@software: PyCharm
@file: putNNKafka.py
@time: 2020-06-20 09:56
'''
# -*- coding: UTF-8 -*-
import json
import os
import sys
import re
import datetime
from kafka import KafkaConsumer
from kafka import KafkaProducer
reload(sys)
sys.setdefaultencoding('utf-8')
import time
import lmdb
import requests

#query = sys.argv[1]

bootstrap_servers = ["172.21.192.38:9092","172.21.192.2:9092","172.21.192.81:9092"]
topic = "kanzhun.de_six.jerusalem.spider_gsxt_query"
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

bn = datetime.datetime.today()

for line in sys.stdin:
    query = line[:-1]
    send_k = json.dumps({"query": query, "site": "gsxt", "time":bn.strftime("%Y-%m-%d %H:%M:%S"), "user": "liuchang", "taskname": "creditcode_company"},ensure_ascii=False)
    print("Sent gsxt: " + send_k)
    producer.send(topic, send_k)
    producer.flush()