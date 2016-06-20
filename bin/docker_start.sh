#!/bin/bash

REGISTRY=registry.docker.dev.sogou-inc.com:5000
IMAGE=beaver/beaver-service
VERSION=1.0-SNAPSHOT

PORT=9080
ADMIN_PORT=9081
LOG_DIR=/search/ted/beaver/logs
DATA_DIR=/search/ted/beaver/data

docker run -d \
  -p $PORT:8080 -p $ADMIN_PORT:8081 \
  -v $LOG_DIR:/search/beaver/logs \
  -v $LOG_DIR:/search/beaver/data \
  $REGISTRY/$IMAGE:$VERSION