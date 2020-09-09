#!/usr/bin/env bash

export HOST_IP=${HOST_IP:-`ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.*'`}
export SERVER_PORT=${SERVER_PORT:-8080}
APP_ROOT=${PWD}/../configs
export HZ_MEMBER_IPS=${HZ_MEMBER_IPS:-${HOST_IP}}
export CASSANDRA_SEED_IPS=${CASSANDRA_SEED_IPS:-${HOST_IP}}
export LOGS_DIR=${LOGS_DIR:-${APP_ROOT}/../../../bigben_logs}

NUM_INSTANCES=${NUM_INSTANCES:-1}
HZ_PORT=${HZ_PORT:-5701}

echo HOST_IP: ${HOST_IP}, SERVER_PORT: ${SERVER_PORT}, \
HZ_MEMBER_IPS: ${HZ_MEMBER_IPS}, CASSANDRA_SEED_IPS: ${CASSANDRA_SEED_IPS}, \
HZ_PORT: ${HZ_PORT}, NUM_INSTANCES: ${NUM_INSTANCES}

DEFAULT_JAVA_OPTS="-server -XX:+UnlockExperimentalVMOptions -XX:InitialRAMFraction=2 -XX:MinRAMFraction=2 -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+UseStringDeduplication"
JAVA_OPTS=${JAVA_OPTS}
echo "using JAVA_OPTS: ${JAVA_OPTS}"

if [[ "x${JAVA_OPTS}" != "x" ]]; then
    export JAVA_OPTS="${DEFAULT_JAVA_OPTS} ${JAVA_OPTS}"
else
    export JAVA_OPTS="${DEFAULT_JAVA_OPTS}"
fi

echo "starting ${NUM_INSTANCES} app node(s)"

i=1
while [[  ${i} -lt $(($NUM_INSTANCES + 1)) ]]; do
    app_port=$((${SERVER_PORT} + 101 * $((i - 1))))
    hz_port=$((${HZ_PORT} + i - 1))
    echo "starting node $i at app port: $app_port, hz port: $hz_port, logs: ${LOGS_DIR}/bigben_app_${app_port}.log"
    LOG_FILE="${LOGS_DIR}/bigben_app_${app_port}.log"
    java ${JAVA_OPTS} \
        -Dbigben.log.config=${APP_ROOT}/log4j.xml \
        -Dbigben.log.file=${LOG_FILE} \
        -Dapp.server.port=${app_port} \
        -Dbigben.configs="uri://${APP_ROOT}/overrides.yaml,uri://${APP_ROOT}/../bin/bigben.yaml" \
        -Dhz.network.port=${hz_port} \
        -jar ../bin/bigben.jar > /dev/null &
    if [[ ${NUM_INSTANCES} == 1 ]]; then
       tail -f ${LOG_FILE}
    fi
    let i=i+1
done
