#!/bin/bash

dir=`dirname $0`
dir=`cd $dir/..; pwd`

if [ $# -lt 1 ]; then
  echo "usage: $0 <jobId> [executor num]"
  exit 1
fi

jobId=$1
executorNum=2
if [ $# -ge 2 ]; then
  executorNum=$2
fi

hdfsRoot=beaver/output
hdfsOutput=$hdfsRoot/$jobId
hdfsSchemaOutput=$hdfsOutput/schema
hdfsDataOutput=$hdfsOutput/data

hadoop fs -mkdir -p $hdfsRoot
hadoop fs -rm -r $hdfsOutput

# TODO support dynamic pass the spark config
cat - | spark-submit \
  --name beaver-$jobId \
  --master yarn-client \
  --driver-memory 1G \
  --conf spark.yarn.am.memory=1G \
  --executor-memory 2G \
  --num-executors $executorNum \
  --class com.sogou.spark.sql.SparkSQLCollector \
  $dir/bin/spark-sql-collector $hdfsOutput

if [ $? -ne 0 ]; then
  echo "failed to run the job: $jobId"
  exit 1
fi

localOutput=$dir/data/$jobId
localSchemaOutput=$localOutput/$jobId.schema
localDataOutput=$localOutput/$jobId.data

rm -f $localOutput
mkdir -p $localOutput

hadoop fs -getmerge $hdfsSchemaOutput/part-* $localSchemaOutput
hadoop fs -getmerge $hdfsDataOutput/part-* $localDataOutput