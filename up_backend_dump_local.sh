#!/bin/bash

set -e

docker-compose -f docker-compose_dump.local.yml down -v
COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -f docker-compose_dump.local.yml build --no-cache --parallel
docker-compose -f docker-compose_dump.local.yml up -d

sleep 60
./setup_db_dump.sh
