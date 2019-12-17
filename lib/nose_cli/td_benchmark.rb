# frozen_string_literal: true

require 'csv'
require 'table_print'

module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor
      desc 'benchmark PLAN_FILE', 'test performance of plans in PLAN_FILE'

      long_desc <<-LONGDESC
        `nose benchmark` will take a JSON file output by `nose search`,
        execute each statement, and output a summary of the execution times.
        Before runnng benchmark, `nose create` and `nose load` must be used to
        prepare the target database.
      LONGDESC

      # by benchmark command
      shared_option :mix
      option :num_iterations, type: :numeric, default: 100,
             banner: 'ITERATIONS',
             desc: 'the number of times to execute each ' \
                                    'statement'
      option :repeat, type: :numeric, default: 1,
             desc: 'how many times to repeat the benchmark'
      option :group, type: :string, default: nil, aliases: '-g',
             desc: 'restrict the benchmark to statements in the ' \
                           'given group'
      option :fail_on_empty, type: :boolean, default: true,
             desc: 'abort if a column family is empty'
      option :totals, type: :boolean, default: false, aliases: '-t',
             desc: 'whether to include group totals in the output'
      option :format, type: :string, default: 'txt',
             enum: %w(txt csv), aliases: '-f',
             desc: 'the format of the output data'

      # by create command
      option :dry_run, type: :boolean, default: false,
             desc: 'print the DDL, but do not execute'
      option :skip_existing, type: :boolean, default: false, aliases: '-s',
             desc: 'ignore indexes which already exist'
      option :drop_existing, type: :boolean, default: true,
             desc: 'drop existing indexes before recreation'

      # by load command
      option :progress, type: :boolean, default: true, aliases: '-p',
             desc: 'whether to display an indication of progress'
      option :limit, type: :numeric, default: nil, aliases: '-l',
             desc: 'limit the number of entries loaded ' \
                           '(useful for testing)'
      option :skip_nonempty, type: :boolean, default: true, aliases: '-s',
             desc: 'ignore indexes which are not empty'

      def td_benchmark(plan_file)
        label = File.basename plan_file, '.*'

        result, backend = load_time_depend_plans plan_file, options
        loader = get_class('loader', options).new result.workload, backend
        backend.clear_keyspace
        set_up_db(result, backend, loader)

        (0...result.timesteps).each do |timestep|
          STDERR.puts "\e[33m timestep: #{timestep} ===================================================== \e[0m"
          migration_worker, _ = exec_migration_async(plan_file, result, timestep)
          #exec_migration(result, timestep)

          indexes_for_this_timestep = result.indexes_used_in_plans(timestep)
          index_values = index_values indexes_for_this_timestep, backend,
                                      options[:num_iterations],
                                      options[:fail_on_empty]
          group_tables = Hash.new { |h, k| h[k] = [] }
          group_totals = Hash.new { |h, k| h[k] = 0 }

          result.time_depend_plans.map{|tdp| tdp.plans[timestep]}.each do |plan|
            query = plan.query
            weight = result.workload.time_depend_statement_weights[query]
            next if query.is_a?(SupportQuery) || !weight
            @logger.debug { "Executing #{query.text}" }
            STDERR.puts "Executing Query: #{query.text}"
            STDERR.puts "    Executing Plan: #{plan.inspect}"

            next unless options[:group].nil? || plan.group == options[:group]

            measurement = bench_query backend, plan.indexes, plan, index_values,
                                      options[:num_iterations], options[:repeat],
                                      weight: weight
            next if measurement.empty?

            measurement.estimate = plan.cost
            group_totals[plan.group] += measurement.mean
            group_tables[plan.group] << measurement
          end

          result.workload.updates.each do |update|
            weight = result.workload.time_depend_statement_weights[update]
            next unless weight

            update_plans = result.time_depend_update_plans
                             .map{|tdup| tdup.plans_all_timestep[timestep].plans}
                             .flatten(1)
            plans = (update_plans || []).select do |possible_plan|
              possible_plan.statement == update
            end
            next if plans.empty?

            @logger.debug { "Executing #{update.text}" }
            STDERR.puts "Executing #{update.text}"

            plans.each do |plan|
              next unless options[:group].nil? || plan.group == options[:group]

              # Get all indexes used by support queries
              indexes = plan.query_plans.flat_map(&:indexes) << plan.index

              measurement = bench_update backend, indexes, plan, index_values,
                                         options[:num_iterations],
                                         options[:repeat], weight: weight
              next if measurement.empty?

              measurement.estimate = plan.cost
              group_totals[plan.group] += measurement.mean
              group_tables[plan.group] << measurement
            end
          end

          total = 0
          table = []
          group_totals.each do |group, group_total|
            total += group_total
            total_measurement = Measurements::Measurement.new nil, 'TOTAL'
            group_table = group_tables[group]
            total_measurement << group_table.map{|gt| gt.weighted_mean(timestep)} \
                               .inject(0, &:+)
            group_table << total_measurement if options[:totals]
            table << OpenStruct.new(timestep: timestep, label: label, group: group,
                                    measurements: group_table)
          end

          if options[:totals]
            total_measurement = Measurements::Measurement.new nil, 'TOTAL'
            total_measurement << table.map do |group|
              group.measurements.find { |m| m.name == 'TOTAL' }.mean
            end.inject(0, &:+)
            table << OpenStruct.new(timestep: timestep, label: label, group: 'TOTAL',
                                    measurements: [total_measurement])
          end

          case options[:format]
          when 'txt'
            td_output_table table
          else
            td_output_csv table
          end

          migration_worker.stop
          STDERR.puts "cleanup"
          exec_cleanup(backend, result, timestep)
        end
      end

      private

      def set_up_db(result, backend, loader)
        indexes = result.time_depend_indexes.indexes_all_timestep.first.indexes
        # Produce the DDL and execute unless the dry run option was given
        backend.create_indexes(indexes, !options[:dry_run], options[:skip_existing],
                               options[:drop_existing]).each {|ddl| STDERR.puts ddl}

        # Create a new instance of the loader class and execute
        loader.load indexes, options[:loader], options[:progress],
                    options[:limit], options[:skip_nonempty]
      end

      def exec_migration(plan_file, result, timestep)
        migration_plans = result.migrate_plans.select{|mp| mp.start_time == timestep}

        migration_plans.each do |migration_plan|
          _, backend = load_time_depend_plans plan_file, options
          prepare_next_indexes(migration_plan, backend)
        end
      end

      def exec_cleanup(backend, result, timestep)
        migration_plans = result.migrate_plans.select{|mp| mp.start_time == timestep}
        plans_for_timestep = result.time_depend_plans.map{|tdp| tdp.plans[timestep + 1]}

        migration_plans.each do |migration_plan|
          drop_obsolete_tables(migration_plan, backend, plans_for_timestep)
        end
      end

      # join the value of indexes
      def inner_loop_join(index_values)
        return index_values.to_a.flatten(1)[1] if index_values.length == 1

        result = []
        index_values.each_cons(2) do |former_index, next_index|
          overlap_fields = former_index[0].all_fields & next_index[0].all_fields
          former_index[1].each do |former_value|
            next_index[1].each do |next_value|
              next unless overlap_fields.all? { |overlap_field| former_value[overlap_field.id] == next_value[overlap_field.id]}
              result << former_value.merge(next_value)
            end
          end
        end
        result
      end

      # @param [MigratePlan, Backend]
      def prepare_next_indexes(migrate_plan, backend)
        STDERR.puts "\e[36m migrate from: \e[0m"
        migrate_plan.obsolete_plan&.map{|step| STDERR.puts '  ' + step.inspect}
        STDERR.puts "\e[36m to: \e[0m"
        migrate_plan.new_plan.map{|step| STDERR.puts '  ' + step.inspect}

        migrate_plan.new_plan.steps.each do |new_step|
          next unless new_step.is_a? Plans::IndexLookupPlanStep
          query_plan = migrate_plan.prepare_plans.find{|pp| pp.index == new_step.index}&.query_plan
          next if query_plan.nil?

          values = index_values(query_plan.indexes, backend)
          obsolete_data = inner_loop_join(values)

          unless backend.index_exists?(new_step.index)
            STDERR.puts backend.create_index(new_step.index, !options[:dry_run], options[:skip_existing])
            backend.index_insert(new_step.index, obsolete_data)
          end
        end
      end

      def drop_obsolete_tables(migrate_plan, backend, plans_for_timestep)
        return if migrate_plan.obsolete_plan.nil?
        migrate_plan.obsolete_plan.indexes.each do |index|
          next if plans_for_timestep.any? {|plan| plan.indexes.include? index}

          STDERR.puts "Dropping #{index.key}"
          backend.drop_index(index)
        end
      end

      def exec_migration_async(plan_file, result, timestep)
        migration_worker = NoSE::Worker.new {|_| exec_migration(plan_file, result, timestep)}
        [migration_worker].map(&:run).each(&:join)
        thread = migration_worker.execute
        thread.join
        [migration_worker, thread]
      end

      # Output the table of results
      # @return [void]
      def td_output_table(table)
        columns = [
          'timestep', 'label', 'group',
          { 'measurements.name' => { display_name: 'name' } },
          { 'measurements.weight' => { display_name: 'weight' } },
          { 'measurements.mean' => { display_name: 'mean' } },
          { 'measurements.estimate' => { display_name: 'cost' } }
        ]

        tp table, *columns
      end

      # Output a CSV file of results
      # @return [void]
      def td_output_csv(table)
        csv_str = CSV.generate do |csv|
          csv << %w(timestep label group name weight mean cost)

          table.each do |group|
            group.measurements.each do |measurement|
              csv << [
                group.timestep,
                group.label,
                group.group,
                measurement.name,
                measurement.weight,
                measurement.mean,
                measurement.estimate
              ]
            end
          end
        end

        puts csv_str
      end
    end
  end
end
