#!/bin/bash

set -e

docker-compose down
COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose build --no-cache --parallel
docker-compose up -d

sleep 30
docker exec cassandra_migrate bash create_keyspace.sh
docker exec mysql_tpch bash setup_db.sh
docker exec tpch_reducer ruby delete_cascade_rows.rb
