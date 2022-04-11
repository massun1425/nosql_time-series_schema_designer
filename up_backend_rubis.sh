#!/bin/bash

set -e

docker-compose -f docker-compose_rubis.yml down
COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -f docker-compose_rubis.yml build --no-cache --parallel
docker-compose -f docker-compose_rubis.yml up -d

sleep 100
docker cp ./backend/cassandra/create_keyspace_rubis.sh c1_wakuta:/
docker exec c1_wakuta bash create_keyspace_rubis.sh
docker exec -ti faker_client node fake.js
docker exec -ti mysql_rubis bash -c "mysql -uroot -proot -Drubis < /additional_index.sql"

