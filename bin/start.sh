#!/bin/bash

dir=`dirname $0`
dir=`cd $dir/..; pwd`

mkdir -p $dir/logs/jobs
mkdir -p $dir/data

java -jar $dir/lib/beaver-service-1.0-SNAPSHOT.jar server $dir/conf/beaver.yml