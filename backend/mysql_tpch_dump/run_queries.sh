 for i in $(seq 1 22); do docker exec mysql - time mysql -uroot -proot -D tpch < $i.sql; done
