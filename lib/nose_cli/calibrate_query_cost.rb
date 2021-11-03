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
        queries = []

        # <fast queries>
        queries << Statement.parse('SELECT l_orderkey.* FROM lineitem.l_orderkey.o_custkey '\
                                      'WHERE lineitem.l_orderkey = ?', workload.model)
        queries << Statement.parse('SELECT lineitem.* FROM lineitem.l_orderkey.o_custkey '\
                                      'WHERE lineitem.l_comment = ?', workload.model)

        calibrate_for_queries queries, workload, [1, 10, 50, 100], [0.01, 0.1, 0.5, 1.0]
      end

      private

      def calculate_current_records(record_points)
        record_points_hash = {:rows_coeff => [], :parts_coeff => [], :latency => [], :raw_rows_coeff => [], :raw_field_size => [], :raw_latency => []}
        record_points.each {|rp| rp.keys.each {|k| record_points_hash[k] << rp[k]}}

        hash_array_to_csv record_points_hash

        begin
          multi_regression record_points_hash, [:parts_coeff, :rows_coeff], :latency
        rescue => e
          STDERR.puts e.inspect
        end
      end

      def calibrate_for_queries(queries, workload, get_nums, reduction_ratios)
        record_points_list = queries.flat_map do |q|
          raw_record_points = calibrate_query_coeff q, workload, get_nums, reduction_ratios
          calculate_current_records raw_record_points
          raw_record_points
        end

        calculate_current_records record_points_list
      end

      def calibrate_query_coeff(query, workload, get_nums, reduction_rates)

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
        full_values, loading_time = measure_time {loader.query_for_index(index, options[:loader], false)}
        full_record_size = full_values.size
        STDERR.puts "whole loading time: " + loading_time.to_s

        record_points = []
        loop_times = 1
        loop_times.times do |_|
          record_points += reduction_rates.flat_map do |reduction_rate|
            target_record_size = (full_record_size * reduction_rate).to_i
            puts "start benchmark for #{target_record_size}"
            backend.recreate_index(index, !options[:dry_run], options[:skip_existing], true)

            sampled_values = full_values.sample(target_record_size, random: Object::Random.new(100))
            backend.load_index_by_cassandra_loader index, sampled_values

            #Parallel.map(get_nums, in_processes: Parallel.processor_count / 4) do |get_num|
            get_nums.map do |get_num|
              conditions = get_conditions backend, index, plan, sampled_values, get_num
              elapse = measure_latency backend, plan, conditions
              real_reduction_rate = (target_record_size / index.entries.to_f).round(10)

              {
                :parts_coeff => [parts_coeff * get_num, conditions.size].min,
                :rows_coeff => [rows_coeff * real_reduction_rate * get_num * step.fields.sum_by(&:size),
                                                     conditions.size > 0 ? step.fields.sum_by(&:size) : 0].max, # the number of returned row would not smaller than 1
                :raw_rows_coeff => rows_coeff,
                :raw_field_size => step.fields.sum_by(&:size),
                :latency => [elapse, 0.001].max,
                :raw_latency => elapse
              }
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
          puts "#{x_var}: #{lr.coeffs[x_var]}"
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
        run_without_gc do
          _, elapse = measure_time {conditions.each {|condition| prepared.execute condition}}
          elapse
        end
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
