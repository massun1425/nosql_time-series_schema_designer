#!/bin/bash

set -e

docker-compose down
docker-compose build --no-cache
docker-compose up -d

sleep 20

docker exec cassandra_migrate bash create_keyspace.sh
#docker cp ../../rubis_basic.sql mysql_migrate:/
#docker exec mysql_migrate mysql -uroot -proot -Drubis < /rubis_basic.sql
