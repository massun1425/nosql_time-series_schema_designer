echo "part <-> parsupp"
mysql -uroot -proot -D tpch -e "SELECT COUNT(1) FROM part, partsupp WHERE p_partkey=ps_partkey;"

echo "supplier<-> parsupp"
mysql -uroot -proot -D tpch -e "SELECT COUNT(1) FROM supplier, partsupp WHERE s_suppkey=ps_suppkey;"

echo "lineitem<-> parsupp"
mysql -uroot -proot -D tpch -e "SELECT COUNT(1) FROM lineitem, partsupp WHERE l_partkey=ps_partkey AND l_suppkey = ps_suppkey;"

echo "lineitem<-> orders"
mysql -uroot -proot -D tpch -e "SELECT COUNT(1) FROM lineitem, orders WHERE l_orderkey=o_orderkey;"

echo "customer<-> orders"
mysql -uroot -proot -D tpch -e "SELECT COUNT(1) FROM customer, orders WHERE c_custkey=o_custkey;"

echo "customer<-> nation"
mysql -uroot -proot -D tpch -e "SELECT COUNT(1) FROM customer, nation WHERE c_nationkey=n_nationkey;"

echo "region <-> nation"
mysql -uroot -proot -D tpch -e "SELECT COUNT(1) FROM region, nation WHERE r_regionkey=n_regionkey;"

