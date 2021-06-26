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
        calibrate coefficient value for extracting.
      LONGDESC

      def calibrate_migration_cost
        options[:index_cost] = options[:partition_cost] = options[:row_cost] = 0.1
        workload = Workload.new{|_| Model('tpch_card_key_composite_dup_lineitems_order_customer')}
        #query1 = Statement.parse 'SELECT l_orderkey.*, lineitem.* FROM lineitem.l_orderkey.o_custkey '\
        #                              'WHERE o_custkey.c_mktsegment = ?', workload.model
        query1 = Statement.parse 'SELECT l_orderkey.*, lineitem.* FROM lineitem.l_orderkey.o_custkey '\
                                      'WHERE o_custkey.c_custkey = ?', workload.model
        StackProf.run(mode: :cpu, out: 'stackprof_calibrate_migration_cost.dump', raw: true) do
          extract_coeff = calibrate_extract_coeff query1, workload
        end
      end

      private

      def calibrate_extract_coeff(query, workload)
        index = query.materialize_view
        backend = Backend::CassandraBackend.new(workload.model, [index], nil, nil, options[:backend])
        backend.initialize_client
        backend.clear_keyspace

        plan = Plans::QueryPlanner.new(workload.model,
                                          [index],
                                          Cost::CassandraCost.new({:index_cost => 1, :partition_cost => 1, :row_cost => 1}))
                                     .min_plan query

        loaded_records = setup_index(index, backend, workload)
        conditions = whole_execute_conditions(plan.params, loaded_records)
        conditions = distinct_condition_array(conditions)
        #conditions = whole_execute_conditions(plan.params, loaded_records).uniq
        backend.initialize_client
        prepared = backend.prepare_query nil, plan.select_fields, plan.params, [plan]

        total_retries = 10
        retry_count = 0
        begin
          rows, normal_elapse = measure_time {conditions.flat_map {|condition| prepared.execute condition}}
        rescue
          sleep 10
          retry_count += 1
          backend.initialize_client
          prepared = backend.prepare_query nil, plan.select_fields, plan.params, [plan]
          retry if retry_count < total_retries
          raise
        end
        bulk_rows, bulk_elapse = measure_time {backend.unload_index_by_cassandra_unloader index}

        puts "normal: #{normal_elapse}, bulk: #{bulk_elapse}"
        fail "normal querying and bulk unloading does not match: normal size #{rows.size.to_s}, bulk size #{bulk_rows.size.to_s} "if rows.size != bulk_rows.size
        bulk_elapse / normal_elapse
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

      def setup_index(index, backend, workload)
        loader = get_class('loader', options).new workload, backend
        # Produce the DDL and execute unless the dry run option was given
        backend.create_indexes([index], !options[:dry_run], options[:skip_existing],
                               options[:drop_existing]).each {|ddl| STDERR.puts ddl}

        # Create a new instance of the loader class and execute
        index_values, loading_time = measure_time {loader.load([index], options[:loader], options[:progress], options[:limit], options[:skip_nonempty], -1)}
        fail if index_values.keys.size > 1 and index_values.keys.first == index
        STDERR.puts "whole loading time: " + loading_time.to_s
        records = Backend::CassandraBackend.remove_any_null_place_holder_row index_values[index]
        backend.cast_records(index, records)
      end

      def distinct_condition_array(conditions_list)
        condition_map = {}
        conditions_list.each do |conditions|
          key = conditions.values.map do |c|
            v = c.value.is_a?(Float) ? (c.value * 10000).to_i : c.value
            "#{c.field.inspect} #{c.operator} #{v}"
          end.join(',')
          condition_map[key] = conditions
        end
        condition_map.values
      end

      def measure_time(&block)
        start = Time.now
        res = block.call
        return res , (Time.now - start)
      end
    end
  end
end
