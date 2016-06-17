#!/bin/bash

mvn package
mkdir -p logs/jobs
java -jar target/beaver-service-1.0-SNAPSHOT.jar server conf/beaver.yml