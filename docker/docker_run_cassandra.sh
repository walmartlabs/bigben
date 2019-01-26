#!/usr/bin/env bash
set -e
CASSANDRA_CONTAINER_NAME=bigben_cassandra
CASSANDRA_PORT=9042
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