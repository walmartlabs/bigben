#!/usr/bin/env bash
docker-compose run setup_cassandra
docker exec -i cassandra cqlsh -f /tmp/bigben-schema.cql
docker-compose run bigben