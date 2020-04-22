#!/usr/bin/env bash

HDFS='KG/'
DATE=$(date '+%Y-%m-%d')
TIME_H=`date '+%H'`
# Step1: 删除30天之前的所有数据
# 30 day ago
date=$(date -d '720 hour ago' '+%Y-%m-%d')
time_h=`date -d '720 hour ago' '+%H'`
t_last=`date -d "${date} ${time_h}" +%s`

# 将HDFS中的文件名称遍历，存入文件中
if [ -e monthfiles.txt ];then
  rm -r monthfiles.txt
fi
hadoop fs -ls /user/de_six/KG/ | awk '{print $8}' > monthfiles.txt
# 计算出文件中最久远的时间
oldest_time=`cat monthfiles.txt | python oldest.py`
t_oldest=`date -d "${oldest_time}" +%s`
# 进行循环，从30天前的时间点开始删除文件，直到最小的时间点
while [ $t_oldest -lt $t_last ]
do
lastUp=`date --date="${date} ${time_h} -1 hour" +%Y-%m-%d" "%H`
OLD_IFS="$IFS"
IFS=" "
arr=($lastUp)
IFS="$OLD_IFS"
date=${arr[0]}
time_h=${arr[1]}
t_last=`date -d "${date} ${time_h}" +%s`
hadoop fs -test -e "${HDFS}/KG-basic.BAK.${date}.${time_h}"
if [ $? -eq 0 ];then
hadoop fs -rm -r "${HDFS}/KG-basic.BAK.${date}.${time_h}"
fi
done
