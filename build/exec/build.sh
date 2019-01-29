#!/usr/bin/env bash
set -e
cd ../..
mvn clean install
rm -rf build/bin || true
mkdir build/bin
cp app/target/bigben.jar build/bin/
cp cassandra/src/main/resources/bigben-schema.cql ./build/bin/bigben-schema.cql
cp app/src/main/resources/bigben.yaml ./build/bin/bigben.yaml
