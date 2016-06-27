#!/bin/bash

dir=`dirname $0`
dir=`cd $dir/../..; pwd`

if [ $# -lt 3 ]; then
  echo "usage: $0 <tableName> <startTime> <endTime>"
  exit 1
fi

tableName=$1
startTime=$2
endTime=$3
factor=1.0
min=2
max=100

if [ $# -ge 4 ]; then factor=$4; fi
if [ $# -ge 5 ]; then min=$5; fi
if [ $# -ge 6 ]; then max=$6; fi

spark-submit \
  --master "local[*]" \
  --class com.sogou.spark.sql.GetSparkExecutorNum \
  $dir/bin/ext/spark-sql-collector.jar \
  $tableName $startTime $endTime $factor $min $max 2>&1 | \
  grep "^executors:" | awk -F":" '{print $2}'