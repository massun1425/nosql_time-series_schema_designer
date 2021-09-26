# frozen_string_literal: true

require 'csv'
require 'table_print'
require 'stackprof'

module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor
      desc 'calibrate migration coeff', 'test performance of plans in PLAN_FILE'

      long_desc <<-LONGDESC
        calibrate coefficient value for extracting and loading.
      LONGDESC

      def calibrate_migration_cost
        options[:index_cost] = options[:partition_cost] = options[:row_cost] = 0.1
        workload = Workload.new{|_| Model('tpch_card_key_composite_dup_lineitems_order_customer')}
        query = Statement.parse 'SELECT l_orderkey.*, lineitem.* FROM lineitem.l_orderkey.o_custkey '\
                                      'WHERE o_custkey.c_custkey = ?', workload.model
        calibrate_extract_coeff query, workload
      end

      private

      def calibrate_extract_coeff(query, workload)
        index = query.materialize_view
        backend = Backend::CassandraBackend.new(workload.model, [index], nil, nil, options[:backend])
        backend.initialize_client
        backend.clear_keyspace
        loader = get_class('loader', options).new workload, backend
        full_values, _ = measure_time {loader.query_for_index(index, options[:loader], true)}

        record_points = {:record_size => [], :loading_time => [], :unloading_time => []}
        reduction_rates = (1..5).map{|i| (0.2 * i).round(3)}.reverse
        iteration_times = 5
        (0...iteration_times).each do |_|
          reduction_rates.each do |reduction_rate|
            backend.recreate_index(index, !options[:dry_run], options[:skip_existing], true)
            target_record_size = (full_values.size * reduction_rate).to_i
            current_size = (index.size * (target_record_size / index.entries.to_f)).round(4)
            record_points[:record_size] << current_size

            sampled_values = full_values.sample(target_record_size, random: Object::Random.new(100))
            puts "target record size #{sampled_values.size}"

            _, loading_time = measure_time {|_| backend.load_index_by_cassandra_loader index, sampled_values}
            record_points[:loading_time] << loading_time

            res_values , unloading_time = measure_time {backend.unload_index_by_cassandra_unloader index}
            record_points[:unloading_time] << unloading_time
            fail if sampled_values.size != res_values.size
          end
        end
        puts record_points
        hash_array_to_csv record_points
        multi_regression record_points.slice(:record_size, :loading_time), [:record_size], :loading_time
        multi_regression record_points.slice(:record_size, :unloading_time),[:record_size], :unloading_time
      end

      def measure_time(&block)
        start = Time.now
        res = block.call
        return res , (Time.now - start)
      end
    end
  end
end
