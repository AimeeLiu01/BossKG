#!/bin/bash
export JAVA_HOME="/data/j2sdk"
export SPARK_HOME="/data1/spark/spark"

HDFS="/user/de_six/KG"
TODAY=`date '+%Y-%m-%d'`

FLAG=0
HOUR=1
COUNT=0
DS=`date -d ''${HOUR}' hour ago' '+%Y-%m-%d.%H'`
INPUT1="${HDFS}/shop/meituan.${TODAY}/output.dat"
INPUT2="${HDFS}/KG-basic"
OUTPUT="${HDFS}/DP_joined"
OUTPUT2="${HDFS}/DP_notjoined"
hadoop fs -rmr ${OUTPUT}
hadoop fs -rmr ${OUTPUT2}

spark-submit \
    --master "yarn" \
    --driver-memory 2g \
    --num-executors 50 \
    --executor-cores 4 \
    --executor-memory 8g \
    --deploy-mode "client" \
    --queue "kanzhun.de.six.vip" \
    --conf "spark.default.parallelism=300" \
    --conf "spark.rpc.message.maxSize=1024" \
    --conf "spark.shuffle.file.buffer=128k" \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.shuffle.consolidateFiles=true" \
    leftjoin.py ${INPUT1} ${INPUT2} ${OUTPUT} ${OUTPUT2}

rm -r DP_joined
rm -r DP_notjoined
rm -r DP_All
rm -r DP_resut
hadoop fs -getmerge ${OUTPUT} DP_joined
hadoop fs -getmerge ${OUTPUT2} DP_notjoined
cat DP_joined DP_notjoined > DP_All
cat DP_All | python usedata-0506.py > ${TODAY}
hadoop fs -rm -r KG/Submission-data/local/meituan/${TODAY}
hadoop dfs -put ${TODAY} KG/Submission-data/local/meituan/${TODAY}
