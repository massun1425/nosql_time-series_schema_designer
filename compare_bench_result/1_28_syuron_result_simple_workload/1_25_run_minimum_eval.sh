bundle exec nose td_benchmark ./1_25_simple_workload/minimum_prop.json > ./1_25_simple_workload/bench_res_minimum_prop.txt 2>&1
sleep 100
bundle exec nose td_benchmark ./1_25_simple_workload/minimum_static.json > ./1_25_simple_workload/bench_res_minimum_static.txt 2>&1
sleep 100
bundle exec nose td_benchmark ./1_25_simple_workload/minimum_first.json > ./1_25_simple_workload/bench_res_minimum_first.txt 2>&1
sleep 100
bundle exec nose td_benchmark ./1_25_simple_workload/minimum_last.json > ./1_25_simple_workload/bench_res_minimum_last.txt 2>&1
