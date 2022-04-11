#!/bin/bash

set -e

docker-compose -f docker-compose.local.yml down -v
#COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -f docker-compose.local.yml build --no-cache --parallel
docker-compose -f docker-compose.local.yml build --no-cache --parallel
docker-compose -f docker-compose.local.yml up -d

sleep 60
./setup_db.sh
