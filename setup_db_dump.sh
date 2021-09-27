#!/bin/bash

docker cp ./backend/cassandra/create_keyspace.sh c1_wakuta:/
docker exec c1_wakuta bash create_keyspace.sh
docker exec mysql_tpch /bin/bash -c "mysql -uroot -proot tpch < tpch_sf_1.sql"
#docker exec tpch_reducer ruby delete_cascade_rows.rb
