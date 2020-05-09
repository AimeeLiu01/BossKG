#!/usr/bin/env bash
export JAVA_HOME="/data/j2sdk"
export SPARK_HOME="/data1/spark/spark"

HDFS="/user/de_six/KG"
TODAY=`date '+%Y-%m-%d'`

INPUT="KG/KG-basic"
OUTPUT="${HDFS}/Submission-data/globle/idmapping"

spark-submit \
    --master "yarn" \
    --driver-memory 2g \
    --num-executors 50 \
    --executor-cores 4 \
    --executor-memory 4g \
    --deploy-mode "client" \
    --queue "kanzhun.de.six.vip" \
    --conf "spark.default.parallelism=300" \
    --conf "spark.rpc.message.maxSize=1024" \
    --conf "spark.shuffle.file.buffer=128k" \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.shuffle.consolidateFiles=true" \
    TestBoss.py ${INPUT} ${OUTPUT}