# frozen_string_literal: true

require 'csv'
require 'table_print'
require 'statsample'

module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor
      desc 'calibrate migration coeff', 'test performance of plans in PLAN_FILE'

      long_desc <<-LONGDESC
        calibrate coefficient value for extracting.
      LONGDESC

      def calibrate_query_cost
        options[:index_cost] = options[:partition_cost] = options[:row_cost] = 0.1
        workload = Workload.new{|_| Model('tpch_card_key_composite_dup_lineitems_order_customer')}

        record_points = {:rows_coeff => [], :parts_coeff => [], :latency => []}
        query1 = Statement.parse 'SELECT l_orderkey.*, lineitem.* FROM lineitem.l_orderkey.o_custkey '\
                                      'WHERE o_custkey.c_custkey = ?', workload.model
        record_points = calibrate_query_coeff query1, workload, record_points

        query2 = Statement.parse 'SELECT l_orderkey.*, lineitem.* FROM lineitem.l_orderkey.o_custkey '\
                                      'WHERE o_custkey.c_mktsegment = ?', workload.model
        record_points = calibrate_query_coeff query2, workload, record_points

        hash_array_to_csv record_points
        multi_regression record_points, [:parts_coeff, :rows_coeff], :latency
      end

      private

      def calibrate_query_coeff(query, workload, record_points)

        # get the COUNT of target CF
        # base_cost + a (# of get) + b (# of returned records)
        # create and load CF with specified number of records for several number of records (10%, 20%, 30%, ,,, 90%, 100% => x %)
        # ここの割合が (# of returned records) を　x% 倍した値として，レコードセットを取得する．
        # (# of get) は意図的に変化させる．(# of get) は変化させると，その変化分を (# of returned records) にも反映する必要があるので，注意

        index = query.materialize_view
        backend = Backend::CassandraBackend.new(workload.model, [index], nil, nil, options[:backend])
        loader = get_class('loader', options).new workload, backend

        plan = Plans::QueryPlanner.new(workload.model,
                                       [index],
                                       Cost::CassandraCost.new({:index_cost => 1, :partition_cost => 1, :row_cost => 1}))
                                  .min_plan(query)
        step = plan.first
        rows_coeff, parts_coeff  = step.state.cardinality, step.state.hash_cardinality

        # Produce the DDL and execute unless the dry run option was given
        backend.initialize_client
        backend.recreate_index(index, !options[:dry_run], options[:skip_existing], true)

        #index_values, loading_time = measure_time {loader.load([index], options[:loader], options[:progress], options[:limit], options[:skip_nonempty], -1)}
        full_values, loading_time = measure_time {loader.query_for_index(index, options[:loader], true)}
        full_record_size = full_values.size
        STDERR.puts "whole loading time: " + loading_time.to_s

        loop_times = 3
        get_nums = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40]
        reduction_rates = [1e-6, 5 * 1e-6, 1e-5, 5 * 1e-5, 1e-4, 5 * 1e-4, 1e-3, 5* 1e-3, 0.01, 0.04, 0.07, 0.1].map{|i| i * 0.01}
        loop_times.times do |_|
          get_nums.each do |get_num|
            reduction_rates.each do |reduction_rate|
              # ここで，100% のレコード数と本来の index.entries を比較して，レコード数が少ない場合でも対応してあげる必要がある．ただし，migration_cost の場合と違って，全体のレコード数に対してどのくらい応答レコード数が比例するかは微妙
              target_record_size = (full_record_size * reduction_rate).to_i
              real_reduction_rate = (target_record_size / index.entries.to_f).round(10)
              puts "start benchmark for #{target_record_size}"
              backend.recreate_index(index, !options[:dry_run], options[:skip_existing], true)

              sampled_values = full_values.sample(target_record_size, random: Object::Random.new(100))
              backend.load_index_by_cassandra_loader index, sampled_values

              conditions = get_conditions backend, index, plan, sampled_values, get_num

              elapse = measure_latency backend, plan, conditions

              record_points[:parts_coeff] << [parts_coeff * get_num, conditions.size].min

              # the number of returned row would not smaller than 1
              record_points[:rows_coeff] << [rows_coeff * real_reduction_rate * get_num * step.fields.sum_by(&:size),
                                             conditions.size > 0 ? 1 : 0].max
              record_points[:latency] << elapse
            end
          end
        end
        record_points
      end

      def multi_regression(data_hash, x_vars, y_var)
        puts data_hash
        lr = Statsample::Regression.multiple data_hash.to_dataset, y_var

        puts "================================================="
        puts lr.summary
        puts lr.constant
        x_vars.each do |x_var|
          puts lr.coeffs[x_var]
        end
        puts "================================================="
        return Proc.new do |variable_kv_hash|
          fail unless variable_kv_hash.keys.to_set == x_vars.to_set
          res = lr.constant
          variable_kv_hash.each do |k, v|
            res += lr.coeffs[k] * v
          end
          res
        end
      end

      def hash_array_to_csv(hash_array)
        puts "<csv>"
        keys = hash_array.keys
        puts keys.join(',')
        (0...hash_array.values.first.size).each do |idx|
          r = keys.map do |k|
            hash_array[k][idx]
          end.join(',')
          puts r
        end
        puts "</csv>"
      end

      def measure_latency(backend, plan, conditions)
        #conditions = get_conditions backend, index, plan, sampled_values, iteration
        backend.initialize_client
        prepared = backend.prepare_query nil, plan.select_fields, plan.params, [plan]
        puts "condition_size : #{conditions.size.to_s}"
        _, elapse = measure_time {conditions.flat_map {|condition| prepared.execute condition}}
        elapse
      end

      def get_conditions(backend, index, plan, rows, iteration)
        rows = backend.cast_records(index, Backend::CassandraBackend.remove_any_null_place_holder_row(rows))

        conditions = whole_execute_conditions(plan.params, rows.take(iteration))
        puts "====== " + conditions.size.to_s
        conditions
      end

      # create conditions for each index_values
      def whole_execute_conditions(params, values)
        values.map do |value|
          Hash[params.map do |field_id, condition|
            [
              field_id,
              Condition.new(condition.field, condition.operator, value[condition.field.id])
            ]
          end]
        end
      end
    end
  end
end
