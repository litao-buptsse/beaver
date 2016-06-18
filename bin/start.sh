#!/bin/bash

dir=`dirname $0`
dir=`cd $dir/..; pwd`

cd $dir; mvn package

mkdir -p $dir/logs/jobs
java -jar $dir/target/beaver-service-1.0-SNAPSHOT.jar server $dir/conf/beaver.yml