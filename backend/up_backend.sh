#!/bin/bash

set -e

docker-compose down
docker-compose build --no-cache
docker-compose up -d

sleep 70
docker exec cassandra_migrate bash create_keyspace.sh
docker exec mysql_migrate bash setup_db.sh

#docker cp ../../rubis_basic.sql mysql_migrate:/
#docker exec mysql_migrate mysql -uroot -proot -Drubis < /rubis_basic.sql
