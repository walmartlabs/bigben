#!/usr/bin/env bash
set -e
cd ..
mvn clean install
docker build -f docker/Dockerfile -t sandeepmalik/bigben:1 .
docker push sandeepmalik/bigben:1