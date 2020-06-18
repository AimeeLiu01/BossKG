#!/bin/bash
script_abs=$(readlink -f "$0")
script_dir=$(dirname $script_abs)
CONF=$script_dir/../conf/
DICT=$script_dir/../dict/
TOOLS=$script_dir/../tools/
source ${CONF}/conf.sh
export JAVA_HOME="/data/j2sdk"
export HADOOP_CONF_DIR="/etc/hadoop/conf/"
source  /etc/profile


#  后面添加上数据准入框架代码
REFALSH_PATH="${HDFS}/KG-reflash-data/ds=${OUT_DATE}/hour=${OUT_TIME_H}"
CHECK_PATH="${HDFS}/KG-check-data/ds=${OUT_DATE}/hour=${OUT_TIME_H}"
hadoop fs -rmr ${CHECK_PATH}

spark-submit \
    --master "yarn" \
    --driver-memory 2g \
    --num-executors 50 \
    --executor-cores 4 \
    --executor-memory 4g \
    --queue "kanzhun.de.six.vip" \
    --py-files "${TOOLS}checkDataAll2.py" \
    --conf "spark.default.parallelism=3000" \
    --conf "spark.rpc.message.maxSize=1024" \
    --conf "spark.shuffle.file.buffer=128k" \
    --conf "spark.yarn.executor.memoryOverhead=4g" \
    --conf "spark.shuffle.consolidateFiles=true" \
    --files ${CONF}schema_mapping.conf  \
    ${TOOLS}checkDataAll2.py ${REFALSH_PATH} ${CHECK_PATH}
if [ $? -ne 0 ]
then
    exit 1
fi
