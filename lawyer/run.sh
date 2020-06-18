#!/bin/bash
export JAVA_HOME="/data/j2sdk"
export SPARK_HOME="/data1/spark/spark"

HDFS="/user/de_six/KG"
TODAY=`date '+%Y-%m-%d'`
YESTERDAY=$(date -d '24 hour ago' '+%Y-%m-%d')
script_abs=$(readlink -f "$0")
script_dir=$(dirname $script_abs)
FLAG=0
HOUR=1
COUNT=0
DS=`date -d ''${HOUR}' hour ago' '+%Y-%m-%d.%H'`
INPUT="/basicdata/kafka/boss/spider/gsxt/web/ds=${YESTERDAY}/*/*/"
OUTPUT="KG/Submission-data/local/12348.gov.cn/"

hadoop fs -rmr ${OUTPUT}"/"${TODAY}

spark-submit \
    --master "yarn" \
    --driver-memory 2g \
    --num-executors 50 \
    --executor-cores 4 \
    --executor-memory 8g \
    --deploy-mode "client" \
    --queue "kanzhun.de.six.vip" \
    --py-files "$script_dir/raw2data.py,$script_dir/objectClean.py,$script_dir/langconv.py,$script_dir/zh_wiki.py" \
    --conf "spark.default.parallelism=300" \
    --conf "spark.rpc.message.maxSize=1024" \
    --conf "spark.shuffle.file.buffer=128k" \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.shuffle.consolidateFiles=true" \
    --files $script_dir/schema_mapping.conf,$script_dir/schema_clean.conf  \
    run.py ${INPUT} ${OUTPUT}