per=$1
storage_const=$2

bundle exec nose td_benchmark ./1_30_tpch_22q_even_odd_various_str_const/${per}per/1_30_tpch_22q_split_even_odd_${per}_${storage_const}_first.json > ./1_30_tpch_22q_even_odd_various_str_const/${per}per/bench_${per}per_${storage_const}_first.txt 2>&1
sleep 200
bundle exec nose td_benchmark ./1_30_tpch_22q_even_odd_various_str_const/${per}per/1_30_tpch_22q_split_even_odd_${per}_${storage_const}_last.json > ./1_30_tpch_22q_even_odd_various_str_const/${per}per/bench_${per}per_${storage_const}_last.txt 2>&1
sleep 200
bundle exec nose td_benchmark ./1_30_tpch_22q_even_odd_various_str_const/${per}per/1_30_tpch_22q_split_even_odd_${per}_${storage_const}_static.json > ./1_30_tpch_22q_even_odd_various_str_const/${per}per/bench_${per}per_${storage_const}_static.txt 2>&1
sleep 200
bundle exec nose td_benchmark ./1_30_tpch_22q_even_odd_various_str_const/${per}per/1_30_tpch_22q_split_even_odd_${per}_${storage_const}_prop.json > ./1_30_tpch_22q_even_odd_various_str_const/${per}per/bench_${per}per_${storage_const}_prop.txt 2>&1

