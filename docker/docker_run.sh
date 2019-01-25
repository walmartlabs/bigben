#!/usr/bin/env bash
set -e
CASSANDRA_CONTAINER_NAME=bigben_cassandra
APP_CONTAINER_NAME=bigben_app
CASSANDRA_PORT=9042
SERVER_PORT=8080
HZ_PORT=5701
NUM_INSTANCES=${NUM_INSTANCES:-1}
HOST_IP=${HOST_IP:-`ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1'`}
echo "determined host ip: $HOST_IP"
echo "stopping running cassandra, if any"
docker stop ${CASSANDRA_CONTAINER_NAME} || true
echo "starting cassandra"
docker run -d -p ${CASSANDRA_PORT}:${CASSANDRA_PORT} --rm --name ${CASSANDRA_CONTAINER_NAME} -h cassandra cassandra
CASSANDRA_DOCKER_IP=`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${CASSANDRA_CONTAINER_NAME}`
echo "cassandra docker ip: ${CASSANDRA_DOCKER_IP}"
echo "waiting for cassandra to boot up"
docker run --rm dadarek/wait-for-dependencies ${CASSANDRA_DOCKER_IP}:${CASSANDRA_PORT}
echo "copying bigben schema file"
docker cp ../cassandra/src/main/resources/bigben-schema.cql ${CASSANDRA_CONTAINER_NAME}:/tmp/bigben-schema.cql
echo "creating bigben schema"
docker exec -it ${CASSANDRA_CONTAINER_NAME} cqlsh -f /tmp/bigben-schema.cql
echo "stopping app servers, if any"
i=1
while [[  ${i} -lt $(($NUM_INSTANCES + 1)) ]]; do
    echo "stopping ${APP_CONTAINER_NAME}_$i"
    docker stop "${APP_CONTAINER_NAME}_$i" ||  true
    let i=i+1
done
echo "starting ${NUM_INSTANCES} app node(s)"
i=1
while [[  ${i} -lt $(($NUM_INSTANCES + 1)) ]]; do
    app_port=$((${SERVER_PORT} + 101 * $((i - 1))))
    hz_port=$((${HZ_PORT} + i - 1))
    echo "starting ${APP_CONTAINER_NAME}_$i at app port: $app_port, hz port: $hz_port"
    docker run --rm -p ${app_port}:${SERVER_PORT} -p ${hz_port}:${HZ_PORT} -e HOST_IP="${HOST_IP}" \
    -e CONFIGS='bigben-overrides,bigben-config' \
    -v ${PWD}/../app/src/main/resources/bigben.yaml:/dist/bigben-config.yaml \
    -v ${PWD}/configs/overrides.yaml:/dist/bigben-overrides.yaml \
    -v ${PWD}/configs/log4j.xml:/dist/log4j-overrides.xml \
    -v ${PWD}/configs/hz.xml:/dist/hz.xml \
    -e SERVER_PORT=${SERVER_PORT} -e JAVA_OPTS="-Dhazelcast.local.publicAddress=${HOST_IP}:${hz_port}" \
    --name "${APP_CONTAINER_NAME}_$i" sandeepmalik/bigben:1
    let i=i+1
done
echo "waiting for app servers to boot up"
i=1
while [[  ${i} -lt $(($NUM_INSTANCES + 1)) ]]; do
    app_server_docker_ip=`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${APP_CONTAINER_NAME}_$i"`
    echo "waiting for app server ${APP_CONTAINER_NAME}_$i, docker ip: $app_server_docker_ip"
    docker run --rm dadarek/wait-for-dependencies ${app_server_docker_ip}:${SERVER_PORT}
    let i=i+1
done
