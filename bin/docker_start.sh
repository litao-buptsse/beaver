#!/bin/bash

REGISTRY=registry.docker.dev.sogou-inc.com:5000
IMAGE=beaver/beaver-service
VERSION=1.0-SNAPSHOT

LOG_DIR=/search/ted/beaver/logs
DATA_DIR=/search/ted/beaver/data

docker run -d --net=host \
  -v /etc/localtime:/etc/localtime \
  -v $LOG_DIR:/search/beaver/logs \
  -v $LOG_DIR:/search/beaver/data \
  $REGISTRY/$IMAGE:$VERSION
