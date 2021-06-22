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

        #hoge = {:rows_coeff=>[1.2000000000000002, 2.4000000000000004, 3.6, 4.800000000000001, 6.000000000000001, 7.2, 8.4, 9.600000000000001, 10.8, 12.000000000000002, 24.000000000000004, 48.00000000000001, 72.0, 96.00000000000001, 120.00000000000001, 144.0, 168.0, 192.00000000000003, 216.00000000000003, 240.00000000000003, 48.00000000000001, 96.00000000000001, 144.0, 192.00000000000003, 240.00000000000003, 288.0, 336.0, 384.00000000000006, 432.00000000000006, 480.00000000000006, 72.0, 144.0, 216.00000000000003, 288.0, 360.00000000000006, 432.00000000000006, 504.00000000000006, 576.0, 648.0, 720.0000000000001, 96.00000000000001, 192.00000000000003, 288.0, 384.00000000000006, 480.00000000000006, 576.0, 672.0, 768.0000000000001, 864.0000000000001, 960.0000000000001, 120.00000000000001, 240.00000000000003, 360.00000000000006, 480.00000000000006, 600.0, 720.0000000000001, 840.0000000000001, 960.0000000000001, 1080.0, 1200.0], :parts_coeff=>[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100], :latency=>[0.1254168, 0.290245076, 0.510995994, 0.650161744, 1.023588668, 0.890798452, 1.035905282, 1.686579702, 1.827254211, 1.607393893, 2.890128689, 6.459064, 9.784445705, 12.429131227, 17.397597898, 20.754274796, 25.409873602, 28.711113917, 33.939137106, 37.768446642, 6.170795757, 13.498091145, 20.768822568, 28.059961135, 36.135519385, 45.452490052, 52.360789489, 61.389790162, 71.792790538, 80.800086901, 9.325853961, 21.276632783, 33.823378304, 44.621585506, 57.146901499, 72.774409834, 85.846744503, 100.266972549, 114.650407869, 131.086746187, 13.488182955, 31.098078192, 48.418877002, 63.306095237, 80.963923285, 102.153707964, 122.460210525, 141.808905497, 160.254752476, 182.429900262, 17.587024825, 40.347383252, 62.455001801, 84.868739606, 107.094077746, 136.983183327, 158.979438907, 184.849529977, 218.182123127, 251.432478315]}
        #hoge[:rows_coeff] = hoge[:rows_coeff].map{|r| r * (10 ** 5)}
        #hoge[:latency] = hoge[:latency].map{|r| r * 1000}
        #multi_regression hoge, 'latency'.to_sym

        fuga = {:rows_coeff=>[4.0, 8.0, 12.000000000000002, 16.0, 20.0, 24.000000000000004, 28.000000000000004, 32.0, 36.0, 40.0, 80.0, 160.0, 240.00000000000003, 320.0, 400.0, 480.00000000000006, 560.0000000000001, 640.0, 720.0000000000001, 800.0, 160.0, 320.0, 480.00000000000006, 640.0, 800.0, 960.0000000000001, 1120.0000000000002, 1280.0, 1440.0000000000002, 1600.0, 240.00000000000003, 480.00000000000006, 720.0000000000001, 960.0000000000001, 1200.0, 1440.0000000000002, 1680.0000000000002, 1920.0000000000002, 2160.0, 2400.0, 320.0, 640.0, 960.0000000000001, 1280.0, 1600.0, 1920.0000000000002, 2240.0000000000005, 2560.0, 2880.0000000000005, 3200.0, 400.0, 800.0, 1200.0, 1600.0, 2000.0, 2400.0, 2800.0, 3200.0, 3600.0000000000005, 4000.0], :parts_coeff=>[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100], :latency=>[0.003600318, 0.002240111, 0.003082485, 0.002938022, 0.002907705, 0.004691138, 0.005226735, 0.005233388, 0.005390272, 0.003875064, 0.01837556, 0.023607755, 0.02211546, 0.02031692, 0.025066707, 0.02736087, 0.031640604, 0.033297228, 0.037168656, 0.042013122, 0.033352633, 0.040307727, 0.046719739, 0.046287597, 0.041169988, 0.055660536, 0.065486897, 0.067635015, 0.054932698, 0.070276149, 0.043372208, 0.05617341, 0.062718684, 0.065177536, 0.067632962, 0.083088854, 0.088261798, 0.092448499, 0.10806884, 0.113849673, 0.059429392, 0.070141227, 0.082454704, 0.083493589, 0.086891629, 0.109559536, 0.127554164, 0.14154253, 0.104382129, 0.137824518, 0.082592952, 0.091195781, 0.090648896, 0.103198936, 0.108890658, 0.118724962, 0.171927095, 0.167515935, 0.158145779, 0.177632581]}
        model = multi_regression fuga, 'latency'.to_sym
        model

        #query1 = Statement.parse 'SELECT l_orderkey.*, lineitem.* FROM lineitem.l_orderkey.o_custkey '\
        #                              'WHERE o_custkey.c_mktsegment = ?', workload.model
        #extract_coeff = calibrate_query_coeff query1, workload

        #query2 = Statement.parse 'SELECT l_orderkey.*, lineitem.* FROM lineitem.l_orderkey.o_custkey '\
        #                              'WHERE o_custkey.c_custkey = ?', workload.model
        #extract_coeff = calibrate_query_coeff query2, workload
      end

      private

      def calibrate_query_coeff(query, workload)

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

        record_points = {:rows_coeff => [], :parts_coeff => [], :latency => []}
        iterations = [1, 20, 40, 60, 80, 100]
        ここで，小さいレコードから初めすぎて，線型回帰しづらくなっている可能性がありそう
        reduction_rates = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        iterations.each do |iteration|
          reduction_rates.each do |reduction_rate|
            target_record_size = (full_record_size * reduction_rate).to_i
            puts "start benchmark for #{target_record_size}"
            backend.recreate_index(index, !options[:dry_run], options[:skip_existing], true)

            #sampled_values, loading_time = measure_time {loader.load_sampled_record_to_index(index, options[:loader], target_record_size, true)}
            sampled_values = full_values.sample(target_record_size, random: Object::Random.new(100))
            backend.load_index_by_cassandra_loader index, sampled_values

            elapse = measure_latency backend, index, plan, sampled_values, iteration

            record_points[:rows_coeff] << rows_coeff * reduction_rate * iteration
            record_points[:parts_coeff] << parts_coeff * iteration
            record_points[:latency] << elapse
          end
        end
        #puts record_points.inspect
        #lr = Statsample::Regression.multiple(record_points.to_dataset, 'latency'.to_sym)
        #puts lr.summary
        multi_regression record_points, 'latency'.to_sym
      end

      def multi_regression(data_hash, y_var)
        puts data_hash
        #patterns = [10** -7, 10 ** -6, 10 ** -5, 10 ** -4, 10 ** -3, 10 ** -2, 10 ** -1, 1, 10 ** 1, 10 ** 2]
        #patterns.each do |pp|
        #  patterns.each do |rp|
            d = data_hash.dup
        #    d[:parts_coeff] = data_hash[:parts_coeff].map{|r| (r * pp).to_f}
        #    d[:rows_coeff] = data_hash[:rows_coeff].map{|r| (r * rp).to_f}

            lr = Statsample::Regression.multiple d.to_dataset, y_var
            #next unless lr.coeffs[:parts_coeff] > 0 and lr.coeffs[:rows_coeff] > 0

        puts "================================================="
        #puts lr.summary
        #puts lr.coeffs[:parts_coeff] * pp
        #puts lr.coeffs[:rows_coeff] * rp
        puts lr.constant
        puts lr.coeffs[:parts_coeff]
        puts lr.coeffs[:rows_coeff]
        puts "================================================="
        return Proc.new do |part, row|
          lr.constant + lr.coeffs[:parts_coeff] * part + lr.coeffs[:rows_coeff] * row
        end
      end

      def measure_latency(backend, index, plan, sampled_values, iteration)
        conditions = get_conditions backend, index, plan, sampled_values, iteration
        backend.initialize_client
        prepared = backend.prepare_query nil, plan.select_fields, plan.params, [plan]
        _, elapse = measure_time {conditions.flat_map {|condition| prepared.execute condition}}
        elapse
      end

      def get_conditions(backend, index, plan, rows, iteration)
        rows = backend.cast_records(index, Backend::CassandraBackend.remove_any_null_place_holder_row(rows))

        #conditions = distinct_condition_array(whole_execute_conditions(plan.params, values.take(iteration * 10))).take(iteration)
        conditions = distinct_condition_array(whole_execute_conditions(plan.params, rows.take(iteration * 10))).take(iteration)
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

      def measure_time(&block)
        start = Time.now
        res = block.call
        return res , (Time.now - start)
      end


      def distinct_condition_array(conditions_aray_maps)
        conditions_list = conditions_aray_maps.map{|c| c.values}

        condition_map = {}
        conditions_list.each do |conditions|
          key = conditions.map do |c|
            v = c.value.is_a?(Float) ? (c.value * 10000).to_i : c.value
            "#{c.field.inspect} #{c.operator} #{v}"
          end.join(',')
          condition_map[key] = conditions
        end
        condition_map.values
      end
    end
  end
end
