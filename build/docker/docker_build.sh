#!/usr/bin/env bash
set -e
../exec/build.sh
cd ../..
docker build -f build/docker/Dockerfile -t sandeepmalik/bigben:1 .
cd build/docker