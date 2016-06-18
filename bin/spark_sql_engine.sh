#!/bin/bash

dir=`dirname $0`
dir=`cd $dir/..; pwd`

if [ $# -ne 1 ]; then
  echo "usage: $0 <jobId>"
  exit 1
fi

jobId=$1
hdfsRoot=beaver/output
hdfsOutput=$hdfsRoot/$jobId
localOutput=$dir/data/$jobId.data

hadoop fs -mkdir -p $hdfsRoot
hadoop fs -rm -r $hdfsOutput

# TODO support dynamic pass the spark config
cat - | spark-submit \
  --master "local[*]" \
  --class com.sogou.spark.sql.SparkSQLCollector \
  $dir/bin/spark-sql-collector $hdfsOutput

if [ $? -ne 0 ]; then
  echo "failed to run the job: $jobId"
  exit 1
fi

rm -f $localOutput
hadoop fs -getmerge $hdfsOutput/part-* $localOutput