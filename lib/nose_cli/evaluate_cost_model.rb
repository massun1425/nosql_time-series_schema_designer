# frozen_string_literal: true

module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor

      desc 'evaluate cost model', 'compare estimated query cost and the actual query plan latency'

      def evaluate_cost_model
        model_name = 'tpch_card_key_composite_dup_lineitems_order_customer'
        query =  'SELECT l_orderkey.o_orderkey, sum(lineitem.l_extendedprice), sum(lineitem.l_discount), l_orderkey.o_orderdate, l_orderkey.o_shippriority '\
             'FROM lineitem.l_orderkey.o_custkey '\
             'WHERE o_custkey.c_mktsegment = ? AND l_orderkey.o_orderdate < ? AND lineitem.l_shipdate > ? '\
             'ORDER BY lineitem.l_extendedprice, lineitem.l_discount, l_orderkey.o_orderdate ' \
             'GROUP BY l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_shippriority -- Q3'

        plan_selector = Proc.new{|ps| ps.select{|p| p.indexes.map(&:key).to_set >= Set.new(["i283097248", "i2011425508"])}}
        evaluate_cost_model_for_query model_name, query, 1, plan_selector
      end

      private

      def evaluate_cost_model_for_query(model_name, query, iterations, proc)
        workload = Workload.new{|_| Model(model_name)}
        query = Statement.parse query, workload.model

        workload.add_statement query
        cost_model = get_class_from_config options, 'cost', :cost_model

        plans = enumerate_plans_for_query(workload, cost_model, query)
        chosen_plans = chose_plans plans, proc
        puts "=================="
        chosen_plans.each do |plan|
          puts "plan description ========================="
          plan.each { |step| puts '  ' + step.inspect }
          plan.each { |step| step.calculate_cost(cost_model) }

          puts "/ plan description ========================="
          plan.steps.each {|s| s.calculate_cost cost_model}
          puts "estimated cost #{plan.cost}"

          condition_hash = [{"customer_c_mktsegment"=> 'BUILDING', "orders_o_orderdate"=> Date.new(1996, 05, 19), "lineitem_l_shipdate"=> Date.new(1996, 05, 29)}]
          index_values, backend = setup_db_for_cost_model workload, plan, iterations, condition_hash

          measurement = nil
          StackProf.run(mode: :cpu, out: '12_15_stackprof_Q3_baseline_i283097248_i2011425508.dump', raw: true) do
            measurement = bench_query backend, plan.indexes, plan, index_values, iterations
          end
          puts measurement.mean
          puts measurement.values.inspect
          puts "</plan evaluation done ========================="
        end
      end

      def setup_db_for_cost_model(workload, plan, iterations, condition_hash = nil)
        backend = Backend::CassandraBackend.new(workload.model, plan.indexes, nil, nil, options[:backend])
        backend.initialize_client
        loader = get_class('loader', options).new workload, backend
        plan.indexes.each do |idx|
          backend.recreate_index(idx, !options[:dry_run], options[:skip_existing], true)
        end
        row_index_values = loader.load plan.indexes, options[:loader], nil, nil, nil, iterations
        if condition_hash.nil?
          index_values = row_index_values.map do |i, records|
            records = Backend::CassandraBackend.remove_any_null_place_holder_row records
            Hash[i, backend.cast_records(i, records)]
          end.reduce(&:merge)
        else
          index_values = row_index_values.map do |i, _|
            vs = []
            condition_hash.each_with_index do |c, _|
              current = {}
              i.all_fields.select{|f| c.has_key? f.id}.each {|f| current[f.id] = c[f.id]}
              vs << current
            end
            Hash[i, vs]
          end.reduce(&:merge)
        end
        return index_values, backend
      end

      def enumerate_plans_for_query(workload, cost_model, query)
        options[:enumerator] = "graph_based"
        indexes = GraphBasedIndexEnumerator.new(workload, cost_model, 2, options[:choice_limit]) \
                                            .indexes_for_workload.to_a
        Plans::QueryPlanner.new(workload.model, indexes, cost_model).find_plans_for_query(query)
      end

      def chose_plans(plans, block)
        block.call(plans)
      end
    end
  end
end
