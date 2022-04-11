#!/bin/bash

set -e

docker-compose -f docker-compose_rubis.yml down
docker-compose -f docker-compose_rubis.yml build --no-cache
docker-compose -f docker-compose_rubis.yml up -d

sleep 70
docker exec cassandra_migrate bash create_keyspace_rubis.sh
docker exec -ti faker_client node fake.js

