#!/usr/bin/env bash
set -e
APP_CONTAINER_NAME=${APP_CONTAINER_NAME:-bigben_app}
SERVER_PORT=${SERVER_PORT:-8080}
HZ_PORT=5701
NUM_INSTANCES=${NUM_INSTANCES:-5}
APP_ROOT=/dist
HOST_IP=${HOST_IP:-`ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1'`}
echo "determined host ip: $HOST_IP"

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
        -v ${PWD}/../app/src/main/resources/bigben.yaml:${APP_ROOT}/bigben-config.yaml \
        -v ${PWD}/configs/overrides.yaml:${APP_ROOT}/bigben-overrides.yaml \
        -v ${PWD}/configs/log4j.xml:${APP_ROOT}/log4j-overrides.xml \
        -v ${PWD}/../../bigben_logs:${APP_ROOT}/logs \
        -e HOST_IP="${HOST_IP}" \
        -e CONFIGS='bigben-overrides,bigben-config' \
        -e SERVER_PORT=${SERVER_PORT} \
        -e APP_PORT=${app_port} \
        -e JAVA_OPTS="-Dhazelcast.local.publicAddress=${HOST_IP}:${hz_port}" \
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
