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
        calibrate filtering ration of returned records.
      LONGDESC

      def calibrate_filtering_cost
        options[:index_cost] = options[:partition_cost] = options[:row_cost] = 0.1
        workload = Workload.new{|_| Model('tpch_card_key_composite_dup_lineitems_order_customer')}

        base_query_str = 'SELECT l_orderkey.* FROM lineitem.l_orderkey.o_custkey WHERE lineitem.l_linenumber = ?'
        queries = {
          :base_query => Statement.parse(base_query_str, workload.model),
          :with_filtering_queries => [
            Statement.parse(base_query_str + ' AND lineitem.l_quantity < ?', workload.model),
            Statement.parse(base_query_str + ' AND lineitem.l_extendedprice < ?', workload.model),
            Statement.parse(base_query_str + ' AND lineitem.l_discount < ?', workload.model),
            Statement.parse(base_query_str + ' AND lineitem.l_tax < ?', workload.model),
            #Statement.parse(base_query_str + ' AND lineitem.l_linestatus < ?', workload.model),
            Statement.parse(base_query_str + ' AND lineitem.l_shipdate < ?', workload.model),
            Statement.parse(base_query_str + ' AND lineitem.l_commitdate < ?', workload.model),
            Statement.parse(base_query_str + ' AND lineitem.l_receiptdate < ?', workload.model),
          #Statement.parse(base_query_str + ' AND lineitem.l_shipmode < ?', workload.model),
          #Statement.parse(base_query_str + ' AND lineitem.l_shipinstruct < ?', workload.model),
          #Statement.parse(base_query_str + ' AND lineitem.l_comment < ?', workload.model),
          ]
        }

        validate_all_query_has_same_entity_materialize_view queries[:base_query], queries[:with_filtering_queries]

        # Calculate the ratio of the number of records returned by the query with filtering and the query without filtering.
        # The average value of the filtering column is used as the condition value.
        collect_filtering_reduction_ratio_for_queries queries[:base_query], queries[:with_filtering_queries], workload
      end

      private

      def collect_filtering_reduction_ratio_for_queries(base_query, queries_with_filtering, workload)
        base_values = get_base_materialize_view_values workload, base_query
        eq_condition_values = get_eq_condition_values base_values, base_query, 0
        res = collect_filtering_result_for_query base_query, workload, eq_condition_values
        puts "====================================="
        puts base_query.text
        puts res.first.size
        puts "/====================================="
        reduce_ratios = queries_with_filtering.map do |query|
          filtered_record = collect_filtering_result_for_query query, workload, eq_condition_values
          puts "====================================="
          puts query.text
          puts filtered_record.first.size
          reduce_ratio = filtered_record.first.size.to_f / res.first.size.to_f
          puts "reduction rate: " + reduce_ratio.to_s
          puts "/====================================="
          reduce_ratio
        end
        puts "whole average reduction ratio: " + (reduce_ratios.sum.to_f / reduce_ratios.size.to_f).to_s
      end

      def get_base_materialize_view_values(workload, query)
        base_index = query.materialize_view
        backend = Backend::CassandraBackend.new(workload.model, [base_index], nil, nil, options[:backend])
        loader = get_class('loader', options).new workload, backend
        loader.query_for_index(base_index, options[:loader], false)
      end

      def get_eq_condition_values(full_records, query, idx)
        query.eq_fields.map{|f| Hash[f.id, Condition.new(f, "=".to_sym, full_records.map{|r| r[f.id]}[idx])]}.reduce(&:merge)
      end

      def collect_filtering_result_for_query(query, workload, eq_condition)
        index = query.materialize_view
        plan = Plans::QueryPlanner.new(workload.model,
                                       [index],
                                       Cost::CassandraCost.new({:index_cost => 1, :partition_cost => 1, :row_cost => 1}))
                                  .min_plan(query)
        backend = Backend::CassandraBackend.new(workload.model, [index], nil, nil, options[:backend])
        values = setup_index backend, workload, index

        conditions = get_conditions backend, index, plan, values, eq_condition, 1
        res, _ = measure_latency backend, plan, conditions
        res
      end

      def get_conditions(backend, index, plan, rows, row_eq_conditions, iteration)
        rows = backend.cast_records(index, Backend::CassandraBackend.remove_any_null_place_holder_row(rows))
        not_eq_params = plan.params.select{|k, _| not row_eq_conditions.keys.include?(k)}.map{|k, v| Hash[k, v]}.reduce(&:merge)

        eq_conditions = whole_execute_conditions(row_eq_conditions, rows.take(iteration))
        not_eq_cond = get_average_value_condition not_eq_params, rows
        eq_conditions.map {|e_cond| e_cond.merge(not_eq_cond)}
      end

      # params の各属性について，rows に含まれる値の平均値を条件値として返す
      def get_average_value_condition(params, rows)
        Hash[params.to_a.map do |field_id, condition|
          [
            field_id,
            Condition.new(condition.field, condition.operator, get_average_value(rows, field_id))
          ]
        end]
      end

      def setup_index(backend, workload, index)
        loader = get_class('loader', options).new workload, backend
        # Produce the DDL and execute unless the dry run option was given
        backend.initialize_client
        backend.recreate_index(index, !options[:dry_run], options[:skip_existing], true)

        full_values, loading_time = measure_time do
          values = loader.query_for_index(index, options[:loader], false)
          backend.load_index_by_cassandra_loader index, values
          values
        end
        STDERR.puts "whole loading time: " + loading_time.to_s
        full_values
      end

      def measure_latency(backend, plan, conditions)
        backend.initialize_client
        prepared = backend.prepare_query nil, plan.select_fields, plan.params, [plan]
        run_without_gc do
          measure_time do
            conditions.map {|condition| prepared.execute condition}
          end
        end
      end

      def get_average_value(rows, field_name)
        values = rows.map{|r| r[field_name]}
        case values.first
        when Date
          return Time.at(((values.map{|v| v.to_time.to_i}.sum / values.size.to_f).to_i)).to_date
        else
          return values.sum / values.size.to_f
        end
      end

      def validate_all_query_has_same_entity_materialize_view(base_query, queries_with_filter)
        queries_with_filter.each do |query_with_filter|
          fail "all queries should have the same entities" unless base_query.materialize_view.graph.entities == query_with_filter.materialize_view.graph.entities
          base_eq_conditions = base_query.conditions.select{|k, v| v.operator == "=".to_sym}.keys.to_set
          current_eq_conditions = query_with_filter.conditions.select{|k, v| v.operator == "=".to_sym}.keys.to_set
          fail "all queries should have the same eq conditions" unless base_eq_conditions == current_eq_conditions
        end
      end
    end
  end
end
