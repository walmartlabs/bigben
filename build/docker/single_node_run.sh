#!/usr/bin/env bash
set -e
./cassandra_run.sh
export NUM_INSTANCES=1
./app_run.sh