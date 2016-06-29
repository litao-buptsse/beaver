#!/bin/bash

dir=`dirname $0`
dir=`cd $dir/../..; pwd`

factor=0.2
min=2
max=100

if [ $# -ge 1 ]; then factor=$1; fi
if [ $# -ge 2 ]; then min=$2; fi
if [ $# -ge 3 ]; then max=$3; fi

cat - | spark-submit \
  --master "local[*]" \
  --class com.sogou.spark.sql.GetSparkExecutorNum \
  --driver-memory 2G \
  $dir/bin/ext/spark-sql-collector.jar \
  $tableName $startTime $endTime $factor $min $max 2>&1 | \
  grep "^executors:" | awk -F":" '{print $2}'