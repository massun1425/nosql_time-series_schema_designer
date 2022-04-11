#!/bin/bash

set -e

docker-compose down -v
docker system prune
COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose build --no-cache --parallel
docker-compose up -d

sleep 60
./setup_db.sh
