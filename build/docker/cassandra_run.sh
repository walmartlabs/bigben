#!/usr/bin/env bash
set -e
CASSANDRA_CONTAINER_NAME=${CASSANDRA_CONTAINER_NAME:-bigben_cassandra}
CASSANDRA_PORT=${CASSANDRA_PORT:-9042}
CASSANDRA_GOSSIP_PORT=${CASSANDRA_GOSSIP_PORT:-7000}
HOST_IP=${HOST_IP:-`ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1'`}
echo "determined host ip: $HOST_IP"
echo "stopping ${CASSANDRA_CONTAINER_NAME}, if running"
docker stop ${CASSANDRA_CONTAINER_NAME} || true
echo "starting ${CASSANDRA_CONTAINER_NAME}"
docker run -d --rm \
-p ${CASSANDRA_PORT}:${CASSANDRA_PORT} \
-e CASSANDRA_BROADCAST_ADDRESS=${HOST_IP} \
-p ${CASSANDRA_GOSSIP_PORT}:${CASSANDRA_GOSSIP_PORT} \
-v ${PWD}/../bin/bigben-schema.cql:/tmp/bigben-schema.cql \
--name ${CASSANDRA_CONTAINER_NAME} cassandra
CASSANDRA_DOCKER_IP=`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${CASSANDRA_CONTAINER_NAME}`
echo "${CASSANDRA_CONTAINER_NAME} docker ip: ${CASSANDRA_DOCKER_IP}"
echo "waiting for ${CASSANDRA_CONTAINER_NAME} to boot up"
docker run --rm dadarek/wait-for-dependencies ${CASSANDRA_DOCKER_IP}:${CASSANDRA_PORT}
echo "creating bigben schema"
docker exec -it ${CASSANDRA_CONTAINER_NAME} cqlsh -f /tmp/bigben-schema.cql