#!/usr/bin/env bash
docker-compose run setup_cassandra
docker exec -i cassandra cqlsh -f /tmp/bigben-schema.cql
docker-compose run -p 8080:8080 bigben