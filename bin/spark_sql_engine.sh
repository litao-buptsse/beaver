#!/bin/bash

dir=`dirname $0`
dir=`cd $dir/..; pwd`

if [ $# -ne 1 ]; then
  echo "usage: $0 <jobId>"
  exit 1
fi

jobId=$1
hdfsOutput=beaver/output/$jobId

hadoop fs -rmr $hdfsOutput
hadoop fs -mkdir -p $hdfsOutput

cat - | $dir/bin/spark-sql-collector $hdfsOutput

if [ $? -ne 0 ]; then
  echo "failed to run the job: $jobId"
  exit 1
fi

localOutput=$dir/data/$jobId.data
rm -f $localOutput
hadoop fs -getmerge $hdfsOutput/part-* $localOutput