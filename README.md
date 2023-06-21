# NoSQL Time-Series Schema Designer Cli

## Acknowledgements

This repository is implemented based on the fork of [nose-cli](https://github.com/michaelmior/nose-cli)

## Installation

```
git clone --recursive https://github.com/Y-Wakuta/nosql_time-series_schema_designer_cli.git
cd nosql_time-series_schema_designer_cli
```

### Requirements

 * [Ruby](https://www.ruby-lang.org/) 2+
 * [bundler](http://bundler.io/)
 * [Gurobi](https://www.gurobi.com/) solver (tested with version 10.0.1)

### How to use

1. Store the workload file in `./time_depend_nosql_schema_designer/workloads/`. Specific examples of time depend workload are stored under `./time_depend_nosql_schema_designer/workloads/time_depend/`.
2. Set the absolute path of the `libgurobi91.so` file of the installed gurobi optimizer to the environment variable `GUROBI_LIB`. (tested with gurobi 9.1.1. If the version of gurobi is different, the file name may be different.)
3. By specifying the workload file path in the `bundle exec nose search` command, the optimum schema sequence, query plans, and migration plans for workload are output. The following is an example command for optimizing `./time_depend_nosql_schema_designer/workloads/time_depend/tpch_22q_3_dups_cyclic.rb`, which is an extension of TPC-H to a workload whose execution frequency changes periodically.

    ```shell
    bundle exec nose search time_depend/tpch_22q_3_dups_cyclic
    ```

    You can also specify some options when executing the search command. The following command limits the storage size to the value of environment variable `STORAGE_LIMIT` and outputs the optimization result to time_series_schemas.txt.

    ```shell
    bundle exec nose search time_depend/tpch_22q_3_dups_cyclic --max_space ${STORAGE_LIMIT} > ./time_series_schemas.txt
    ```

4. The optimization result of the search command takes the following format. The optimized result is output in txt format and json format. The json format is used to give optimized result to the td_benchmark command, which is the command to run the benchmark.

    ```txt
    // search output
    <txt format>
    // search result in more readable txt format
    </txt format>
    <json format>
    // seach result in json format to input into benchmark command
    </json format>
    ```

5. As a preliminary preparation for the benchmark, specify the host name and port of mysql and cassandra in `./nose.yml`. And please put dumped tpch data to `./backend/mysql_tpch_dump` directory as `tpch_sf_1.sql.zip`.
6. Run the schema benchmark by extracting the json schema from the output of the search command and inputting it to the td_benchmark command. The following is an example of the command to acquire the benchmark of this file when the file extracted from the json part of time_series_schemas.txt is output to the time_series_schemas.json file.

    ```shell
    bundle exec nose td_benchmark time_series_schemas.json > time_series_benchmark_result.txt
    ```

    The td_benchmark command performs the following processing and measures the response time of each query at each time.
    1. Create time step 0 schema on Cassandra
    2. Get records for each column family from MySQL and then load them into Cassandra
    3. Execute the query for each time step and measure the response time.
    4. In addition, create a schema and load data for the next time step in the migrator process.

7. If you want to extract only the measurement result of the query latency from `time_series_benchmark_result.txt`, you can get the benchmark result of each query as a csv file by `ruby ./compare_bench_result/bench_res_formatter.rb time_series_benchmark_result.txt> time_series_benchmark_result.csv`.
