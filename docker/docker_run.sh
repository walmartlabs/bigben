#!/usr/bin/env bash
docker-compose run -p 9042:9042 setup_cassandra
docker exec -i cassandra cqlsh -f /tmp/bigben-schema.cql
docker-compose run --rm -p 8080:8080 -e CONFIGS='bigben-overrides,bigben-config' -e SERVER_PORT=8080 bigben