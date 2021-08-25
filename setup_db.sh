#!/bin/bash

docker cp ./backend/cassandra/create_keyspace.sh c1_wakuta:/
docker exec c1_wakuta bash create_keyspace.sh
docker exec mysql_tpch bash setup_db.sh
#docker exec tpch_reducer ruby delete_cascade_rows.rb
