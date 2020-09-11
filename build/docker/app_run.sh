#!/usr/bin/env bash
set -e
APP_CONTAINER_NAME=${APP_CONTAINER_NAME:-bigben_app}
SERVER_PORT=${SERVER_PORT:-8080}
HZ_PORT=5701
NUM_INSTANCES=${NUM_INSTANCES:-1}
APP_ROOT=/dist
BUILD_DIR=${PWD}/..
LOGS_DIR=${LOGS_DIR:-${BUILD_DIR}/../../bigben_logs}

HOST_IP=${HOST_IP:-`ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1'`}
CASSANDRA_SEED_IPS=${CASSANDRA_SEED_IPS:-${HOST_IP}}
HZ_MEMBER_IPS=${HZ_MEMBER_IPS:-${HOST_IP}}

DEFAULT_JAVA_OPTS="-server -XX:+UnlockExperimentalVMOptions -XX:InitialRAMFraction=2 -XX:MinRAMFraction=2 -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+UseStringDeduplication"
JAVA_OPTS=${JAVA_OPTS}

if [[ "x${JAVA_OPTS}" != "x" ]]; then
    JAVA_OPTS="${DEFAULT_JAVA_OPTS} ${JAVA_OPTS}"
else
    JAVA_OPTS="${DEFAULT_JAVA_OPTS}"
fi

echo HOST_IP: ${HOST_IP}, SERVER_PORT: ${SERVER_PORT}, \
HZ_MEMBER_IPS: ${HZ_MEMBER_IPS}, CASSANDRA_SEED_IPS: ${CASSANDRA_SEED_IPS}, \
HZ_PORT: ${HZ_PORT}, NUM_INSTANCES: ${NUM_INSTANCES}

function stop() {
    echo "stopping app servers, if any"
    i=1
    while [[  ${i} -lt $(($NUM_INSTANCES + 1)) ]]; do
        app_port=$((${SERVER_PORT} + 101 * $((i - 1))))
        echo "stopping ${APP_CONTAINER_NAME}_$app_port"
        docker stop "${APP_CONTAINER_NAME}_$app_port" || true
        let i=i+1
    done
}

function start() {
    echo "starting ${NUM_INSTANCES} app node(s)"
    i=1
    while [[  ${i} -lt $(($NUM_INSTANCES + 1)) ]]; do
        app_port=$((${SERVER_PORT} + 101 * $((i - 1))))
        hz_port=$((${HZ_PORT} + i - 1))
        echo "starting ${APP_CONTAINER_NAME}_$app_port at app port: $app_port, hz port: $hz_port"
        docker run -d --rm \
        -p ${app_port}:${SERVER_PORT} \
        -p ${hz_port}:${HZ_PORT} \
        -v ${BUILD_DIR}/bin/bigben.yaml:${APP_ROOT}/bigben.yaml \
        -v ${BUILD_DIR}/configs/overrides.yaml:${APP_ROOT}/overrides.yaml \
        -v ${BUILD_DIR}/configs/log4j.xml:${APP_ROOT}/log4j.xml \
        -v ${LOGS_DIR}:${APP_ROOT}/logs \
        -e HOST_IP="${HOST_IP}" \
        -e CASSANDRA_SEED_IPS="${CASSANDRA_SEED_IPS}" \
        -e HZ_MEMBER_IPS="${HZ_MEMBER_IPS}" \
        -e JAVA_OPTS="${JAVA_OPTS} -Dbigben.configs=uri://${APP_ROOT}/overrides.yaml,uri://${APP_ROOT}/bigben.yaml \
        -Dapp.server.port=${SERVER_PORT} \
        -Dbigben.log.file=${APP_ROOT}/logs/bigben_app_${app_port}.log \
        -Dbigben.log.config=${APP_ROOT}/log4j.xml \
        -Dhazelcast.local.publicAddress=${HOST_IP}:${hz_port}" \
        --name "${APP_CONTAINER_NAME}_$app_port" sandeepmalik/bigben:1
        let i=i+1
    done
    echo "waiting for app servers to boot up"
    i=1
    while [[  ${i} -lt $(($NUM_INSTANCES + 1)) ]]; do
        app_server_docker_ip=`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${APP_CONTAINER_NAME}_$app_port"`
        echo "waiting for app server ${APP_CONTAINER_NAME}_$app_port, docker ip: $app_server_docker_ip"
        docker run --rm dadarek/wait-for-dependencies ${app_server_docker_ip}:${SERVER_PORT}
        let i=i+1
    done
}

if [[ $1 == "start" ]]; then
    start
elif [[ $1 == "stop" ]]; then
    stop
else
    stop
    start
fi
