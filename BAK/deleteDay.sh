#!/usr/bin/env bash

HDFS='KG/'
# 当前时间
DATE=$(date '+%Y-%m-%d')
TIME_H=`date '+%H'`

# 24小时前的时间
date=$(date -d '25 hour ago' '+%Y-%m-%d')
time_h=`date -d '25 hour ago' '+%H'`
t_last=`date -d "${date} ${time_h}" +%s`

# 一个月前的时间点
date_m=$(date -d '720 hour ago' '+%Y-%m-%d')
time_m=`date -d '720 hour ago' '+%H'`
t_month=`date -d "${date_m} ${time_m}" +%s`

# 保留每天最大的时间点，存储在dict_time里
if [ -e dayfiles.txt ];then
  rm -r dayfiles.txt
fi
hadoop fs -ls /user/de_six/KG/ | awk '{print $8}' > dayfiles.txt
dict_time=`cat dayfiles.txt | python daymaxdate.py`

# 从一天前的时间点向前删除，时间每次减一小时，直到一个月的时间点
while [ $t_month -lt $t_last ]
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
# 判断该时间点的文件是否存在，如存在
if [ $? -eq 0 ];then
# 再判断该时间点是否在字典中，如果存在，则是该天最大时间，跳过
max_hour=`echo ${dict_time} | python max_hour.py $date`
if [ $max_hour -ne $time_h ];then
# 如果最大小时和当前小时不相同，删除该条文件
hadoop fs -rm -r "${HDFS}/KG-basic.BAK.${date}.${time_h}"
fi
fi
done

