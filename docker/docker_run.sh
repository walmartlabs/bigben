#!/usr/bin/env bash
docker-compose run setup_cassandra
docker exec -i cassandra cqlsh -f /tmp/bigben-schema.cql
docker-compose run --rm -p 8080:8080 bigben