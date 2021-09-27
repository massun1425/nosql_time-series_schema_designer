# frozen_string_literal: true

module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor

      desc 'benchmark PLAN_FILE', 'test performance of plans in PLAN_FILE'

      def evaluate_cost_model
        model_name = 'tpch_card_key_composite_dup_lineitems_order_customer'

        #query =  'SELECT o_custkey.c_custkey, o_custkey.c_name, '\
        #  'sum(lineitem.l_extendedprice), sum(lineitem.l_discount), '\
        #  'o_custkey.c_acctbal, c_nationkey.n_name, '\
        #  'o_custkey.c_address, o_custkey.c_phone, o_custkey.c_comment '\
        # 'FROM lineitem.l_orderkey.o_custkey.c_nationkey '\
        # 'WHERE lineitem.l_returnflag = ? AND l_orderkey.o_orderdate < ?  '\
        # 'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
        # 'GROUP BY o_custkey.c_custkey, o_custkey.c_name, o_custkey.c_acctbal, o_custkey.c_phone, c_nationkey.n_name, o_custkey.c_address, o_custkey.c_comment -- Q10'

        #query  = 'SELECT l_orderkey.o_orderdate, sum(from_lineitem.l_extendedprice), sum(from_lineitem.l_discount) '\
        #    'FROM part.from_partsupp.from_lineitem.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
        #    'WHERE n_regionkey.r_name = ? AND part.p_type = ? AND l_orderkey.o_orderdate < ? ' \
        #    'ORDER BY l_orderkey.o_orderdate ' \
        #    'GROUP BY l_orderkey.o_orderdate -- Q8'

        query =  'SELECT l_orderkey.o_orderkey, sum(lineitem.l_extendedprice), sum(lineitem.l_discount), l_orderkey.o_orderdate, l_orderkey.o_shippriority '\
             'FROM lineitem.l_orderkey.o_custkey '\
             'WHERE o_custkey.c_mktsegment = ? AND l_orderkey.o_orderdate < ? AND lineitem.l_shipdate > ? '\
             'ORDER BY lineitem.l_extendedprice, lineitem.l_discount, l_orderkey.o_orderdate ' \
             'GROUP BY l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_shippriority -- Q3'

        evaluate_cost_model_for_query model_name, query, 1,
                                      #Proc.new{|ps| ps.select{|p| p.indexes.map(&:key).to_set >= Set.new(["i2031405033"])}.first} # i3722636955
                                      Proc.new{|ps| ps.select{|p| p.indexes.map(&:key).to_set >= Set.new(["i3722636955"])}.first} #
      end

      private

      def evaluate_cost_model_for_query(model_name, query, iterations, proc)
        workload = Workload.new{|_| Model(model_name)}
        query = Statement.parse query, workload.model

        workload.add_statement query
        cost_model = get_class_from_config options, 'cost', :cost_model

        plans = enumerate_plans_for_query(workload, cost_model, query)
        plan = choose_plan plans, proc
        puts "plan description ========================="
        plan.each { |step| puts '  ' + step.inspect }
        puts "/ plan description ========================="
        plan.steps.each {|s| s.calculate_cost cost_model}
        puts "estimated cost #{plan.cost}"

        index_values, backend = setup_db_for_cost_model workload, plan, iterations

        #StackProf.run(mode: :cpu, out: 'stackprof_Q10_query_execution_8_22_not_remove_null_place_holder.dump', raw: true) do
        measurement = bench_query backend, plan.indexes, plan, index_values, iterations
        #end
        puts measurement.mean
        puts measurement.values.inspect
      end

      def setup_db_for_cost_model(workload, plan, iterations)
        backend = Backend::CassandraBackend.new(workload.model, plan.indexes, nil, nil, options[:backend])
        backend.initialize_client
        loader = get_class('loader', options).new workload, backend
        plan.indexes.each do |idx|
          backend.recreate_index(idx, !options[:dry_run], options[:skip_existing], true)
        end
        index_values = loader.load plan.indexes, options[:loader], nil, nil, nil, iterations
        index_values = index_values.map do |i, records|
          records = Backend::CassandraBackend.remove_any_null_place_holder_row records
          Hash[i, backend.cast_records(i, records)]
        end.reduce(&:merge)
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
