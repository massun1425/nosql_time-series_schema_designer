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
 * [Gurobi](https://www.gurobi.com/) solver (tested with version 9.1.1)

### How to use

1. ワークロードファイルを `./time_depend_nosql_schema_designer/workloads/` に格納してください．time depend workload の具体例は `./time_depend_nosql_schema_designer/workloads/time_depend/` 以下に格納しています．
2. インストールした gurobi optimizer の `libgurobi91.so` ファイルの絶対パスを環境変数 GUROBI_LIB へ設定してください．(gurobi 9.1.1 で実験していますが，gurobi のバージョンが異なる場合はファイル名が異なる可能性があります)
2. `bundle exec nose search` コマンドにワークロードファイルのパスを指定することで，workload に対して適したスキーマ系列・クエリプラン・マイグレーションプランを出力します．TPC-H を周期的に実行頻度が変化するワークロードへ拡張したワークロードである `./time_depend_nosql_schema_designer/workloads/time_depend/tpch_22q_3_dups_cyclic.rb` を最適化する場合のコマンドの具体例を以下に示します．

    ```shell
    bundle exec nose search time_depend/tpch_22q_3_dups_cyclic
    ```

    また，search コマンドを実行する際に幾つかのオブションを指定することが出来ます．下記のコマンドは，ストレージ容量を $STORAGE_LIMIT の値に制限し，time_series_schemas.txt へ最適化結果を出力しています．

    ```shell
    bundle exec nose search time_depend/tpch_22q_3_dups_cyclic --max_space ${STORAGE_LIMIT} > ./time_series_schemas.txt
    ```

3. search コマンドの最適化結果は以下のフォーマットを取ります．最適化した結果を txt フォーマットと json フォーマットで出力します．json フォーマットは，ベンチマークを実行するコマンドである td_benchmark コマンドへ入力するために使用します．

    ```txt
    // search output
    <txt format>
    // search result in more readable txt format
    </txt format>
    <json format>
    // seach result in json format to input into benchmark command
    </json format>
    ```

4. ベンチマークの事前準備として，mysql と cassandra の host 名と port を `./nose.yml` で指定します．
5. search コマンドの出力から json スキーマを取り出し，td_benchmark コマンドへ入力することで，スキーマのベンチマークを取得します．time_series_schemas.txt の json 部分を抜き出したファイルを time_series_schemas.json ファイルへ出力した場合に，このファイルのベンチマークを取得するコマンドの例を以下に示します．
    ```shell
    bundle exec nose td_benchmark time_series_schemas.json > time_series_benchmark_result.txt
    ```

    td_benchmark コマンドでは以下の処理を行い，各時刻の各クエリの応答時間を計測します．
    1. 時刻0のスキーマを Cassandra 上に作成
    2. MySQL から各 column family のレコードを取得し，Cassandra へロード
    3. 各時刻のクエリを実行し，応答時間を計測
    4. クエリの性能計測のバックエンドで次の時刻に向けたスキーマの作成・データのロードをマイグレータプロセスで実行

6. time_series_benchmark_result.txt からクエリの応答時間の計測結果のみを抜き出す場合は，`ruby ./compare_bench_result/bench_res_formatter.rb time_series_benchmark_result.txt > time_series_benchmark_result.csv` とすることで，csv ファイルとして各クエリのベンチマーク結果を取得できます．


