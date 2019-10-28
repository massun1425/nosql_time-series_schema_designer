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
        sleep(1)
        set_up_db(result, backend, loader)

        (0...result.timesteps).each do |timestep|
          puts "\e[33m timestep: #{timestep} ================================== \e[0m"

          indexes_for_this_timestep = result.time_depend_indexes.indexes_all_timestep[timestep].indexes
          index_values = index_values indexes_for_this_timestep, backend,
                                      options[:num_iterations],
                                      options[:fail_on_empty]
          group_tables = Hash.new { |h, k| h[k] = [] }
          group_totals = Hash.new { |h, k| h[k] = 0 }

          result.time_depend_plans.map{|tdp| tdp.plans[timestep]}.each do |plan|
            query = plan.query
            weight = result.workload.statement_weights[query]
            next if query.is_a?(SupportQuery) || !weight
            @logger.debug { "Executing #{query.text}" }
            puts "Executing Query: #{query.text}"
            puts "    Executing Plan: #{plan.inspect}"

            next unless options[:group].nil? || plan.group == options[:group]

            indexes = plan.select do |step|
              step.is_a? Plans::IndexLookupPlanStep
            end.map(&:index)

            measurement = bench_query backend, indexes, plan, index_values,
                                      options[:num_iterations], options[:repeat],
                                      weight: weight
            next if measurement.empty?

            measurement.estimate = plan.cost
            group_totals[plan.group] += measurement.mean
            group_tables[plan.group] << measurement
          end

          result.workload.updates.each do |update|
            weight = result.workload.statement_weights[update]
            next unless weight

            plans = (result.update_plans || []).select do |possible_plan|
              possible_plan.statement == update
            end
            next if plans.empty?

            @logger.debug { "Executing #{update.text}" }
            puts "Executing #{update.text}"

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
            table << OpenStruct.new(label: label, group: group,
                                    measurements: group_table)
          end

          if options[:totals]
            total_measurement = Measurements::Measurement.new nil, 'TOTAL'
            total_measurement << table.map do |group|
              group.measurements.find { |m| m.name == 'TOTAL' }.mean
            end.inject(0, &:+)
            table << OpenStruct.new(label: label, group: 'TOTAL',
                                    measurements: [total_measurement])
          end

          case options[:format]
          when 'txt'
            output_table table
          else
            output_csv table
          end

          break if timestep == result.timesteps - 1
          exec_migration(result, backend, timestep)


          sleep(1)
        end
      end

      private

      def set_up_db(result, backend, loader)
        indexes = result.time_depend_indexes.indexes_all_timestep.first.indexes
        # Produce the DDL and execute unless the dry run option was given
        backend.create_indexes(indexes, !options[:dry_run], options[:skip_existing],
                                           options[:drop_existing]).each {|ddl| puts ddl}

        # Create a new instance of the loader class and execute
        loader.load indexes, options[:loader], options[:progress],
                    options[:limit], options[:skip_nonempty]
      end

      def exec_migration(result, backend, timestep)
        migration_plans = result.migrate_plans.select{|mp| mp.start_time == timestep}
        migration_plans.each {|mp| prepare_next_indexes(mp, backend) }

        plans_for_timestep = result.time_depend_plans.map{|tdp| tdp.plans[timestep + 1]}
        migration_plans.each do |migration_plan|
          drop_obsolete_tables(migration_plan, backend, plans_for_timestep)
        end
      end

      # @param [MigratePlan, CassandraManager, Array]
      def prepare_next_indexes(migrate_plan, backend)
        puts "\e[36m migrate from: \e[0m"
        migrate_plan.obsolete_plan.map{|step| puts '  ' + step.inspect}
        puts "\e[36m to: \e[0m"
        migrate_plan.new_plan.map{|step| puts '  ' + step.inspect}

        obsolete_data = {}
        migrate_plan.obsolete_plan.steps.each do |obsolete_step|
          obsolete_data.merge!(backend.get_all_data(obsolete_step.index))
        end
        obsolete_rows = []
        (0...obsolete_data.to_a.first[1].size).each do |i|
          tmp_hash = {}
          obsolete_data.each do |k, v|
            tmp_hash[k] = v[i]
          end
          obsolete_rows << tmp_hash
        end

        migrate_plan.new_plan.steps.map{|step| step.index}.each do |new_index|
          unless backend.index_exists?(new_index)
            puts backend.create_index(new_index, !options[:dry_run], options[:skip_existing])
            backend.index_insert(new_index, obsolete_rows)
          end
        end
      end

      def drop_obsolete_tables(migrate_plan, backend, plans_for_timestep)
        migrate_plan.obsolete_plan.steps.map{|step| step.index}.each do |index|
          next if plans_for_timestep.any? {|plan| plan.steps.map{|step| step.index}.include? index}

          backend.drop_index(index)
        end
      end
    end
  end
end
