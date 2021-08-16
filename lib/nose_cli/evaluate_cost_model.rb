# frozen_string_literal: true

module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor

      desc 'benchmark PLAN_FILE', 'test performance of plans in PLAN_FILE'

      def evaluate_cost_model
        model_name = 'tpch_card_key_composite_dup_lineitems_order_customer'
        #query = 'SELECT o_custkey.c_custkey, o_custkey.c_name, '\
        #    'sum(lineitem.l_extendedprice), sum(lineitem.l_discount), '\
        #    'o_custkey.c_acctbal, c_nationkey.n_name, '\
        #    'o_custkey.c_address, o_custkey.c_phone, o_custkey.c_comment '\
        #  'FROM lineitem.l_orderkey.o_custkey.c_nationkey '\
        #  'WHERE lineitem.l_returnflag = ? AND l_orderkey.o_orderdate < ?  '\
        #  'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
        #  'GROUP BY o_custkey.c_custkey, o_custkey.c_name, o_custkey.c_acctbal, o_custkey.c_phone, c_nationkey.n_name, o_custkey.c_address, o_custkey.c_comment -- Q10'

        query = 'SELECT c_nationkey.n_name, sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
          'FROM lineitem.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
          'WHERE n_regionkey.r_name = ? AND l_orderkey.o_orderdate < ? ' \
          'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
          'GROUP BY c_nationkey.n_name -- Q5'

        #query = 'SELECT l_orderkey.o_orderkey, lineitem.l_orderkey, lineitem.l_linenumber, o_custkey.c_custkey FROM lineitem.l_orderkey.o_custkey '\
        #                              'WHERE o_custkey.c_mktsegment = ? GROUP BY l_orderkey.o_orderkey, lineitem.l_orderkey, lineitem.l_linenumber, o_custkey.c_custkey'
        evaluate_cost_model_for_query model_name, query, 10

        #query_no_groupby = 'SELECT l_orderkey.o_orderkey, lineitem.l_orderkey, lineitem.l_linenumber, o_custkey.c_custkey FROM lineitem.l_orderkey.o_custkey '\
        #                              'WHERE o_custkey.c_mktsegment = ?'
        #evaluate_cost_model_for_query model_name, query_no_groupby, 10
      end

      private

      def evaluate_cost_model_for_query(model_name, query, iterations)
        workload = Workload.new{|_| Model(model_name)}
        query = Statement.parse query, workload.model

        workload.add_statement query
        cost_model = get_class_from_config options, 'cost', :cost_model

        plan = choose_plan enumerate_plans_for_query(workload, cost_model, query),
                           Proc.new{|ps| ps.select{|p| p}.first}
        #Proc.new{|ps| ps.find {|p| p.steps.size == 3 and p.first.index.key == "i2954140325"}}
        puts "plan description ========================="
        plan.each { |step| puts '  ' + step.inspect }
        puts "/ plan description ========================="
        plan.steps.each {|s| s.calculate_cost cost_model}
        puts "estimated cost #{plan.cost}"

        index_values, backend = setup_db_for_cost_model workload, plan, iterations
        measurement = bench_query backend, plan.indexes, plan, index_values, iterations
        puts measurement.mean
        puts measurement.values.inspect
      end

      def setup_db_for_cost_model(workload, plan, iterations)
        backend = Backend::CassandraBackend.new(workload.model, plan.indexes, nil, nil, options[:backend])
        loader = get_class('loader', options).new workload, backend
        plan.indexes.each do |idx|
          backend.recreate_index(idx, !options[:dry_run], options[:skip_existing], true)
        end
        index_values = loader.load plan.indexes, options[:loader], nil, nil, nil, iterations
        return index_values, backend
      end

      def enumerate_plans_for_query(workload, cost_model, query)
        options[:enumerator] = "graph_based"
        indexes = enumerate_indexes(workload, cost_model)
        Plans::QueryPlanner.new(workload.model, indexes, cost_model).find_plans_for_query(query)
      end

      def choose_plan(plans, block)
        block.call(plans)
      end
    end
  end
end
