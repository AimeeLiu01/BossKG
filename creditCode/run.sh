#!/usr/bin/env bash
export JAVA_HOME="/data/j2sdk"
export SPARK_HOME="/data1/spark/spark"

HDFS="/user/de_six/KG"
TODAY=`date '+%Y-%m-%d'`
FLAG=0
HOUR=1
COUNT=0
DS=`date -d ''${HOUR}' hour ago' '+%Y-%m-%d'`

rm -r tmp_1
rm -r tmp_2
rm -r tmp_3
rm -r tmp_4
rm -r datapair.${DS}
rm -r creditcode.${DS}
rm -r getK.${DS}
rm -r putK.${DS}
cat priority_1b | python selectGroup.py > tmp_1
cat priority_2b | python selectGroup.py > tmp_2
cat priority_3b | python selectGroup.py > tmp_3
cat priority_4b | python selectGroup.py > tmp_4
cat tmp_1 tmp_2 tmp_3 tmp_4 > datapair.${DS}
cat datapair.${DS} | python cmpt_creditcode.py > creditcode.${DS}

cat creditcode.${DS} | python putNNKafka.py > putK.${DS}

