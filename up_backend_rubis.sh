#!/bin/bash

set -e

docker-compose -f docker-compose_rubis.yml down
COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -f docker-compose_rubis.yml build --no-cache --parallel
docker-compose -f docker-compose_rubis.yml up -d

sleep 50
docker exec cassandra_migrate bash create_keyspace_rubis.sh
docker exec -ti faker_client node one_tenth_fake.js

