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
      option :group, type: :string, default: nil, aliases: '-g',
             desc: 'restrict the benchmark to statements in the ' \
                           'given group'
      option :fail_on_empty, type: :boolean, default: true,
             desc: 'abort if a column family is empty'
      option :totals, type: :boolean, default: false, aliases: '-t',
             desc: 'whether to include group totals in the output'
      option :format, type: :string, default: 'csv',
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
      option :validate_migration, type: :boolean, default: true, aliases: '-v',
             desc: 'whether migration process'

      def td_benchmark(plan_file)
        label = File.basename plan_file, '.*'

        result, backend = load_time_depend_plans plan_file, options
        loader = get_class('loader', options).new result.workload, backend
        backend.clear_keyspace
        setup_db(result, backend, loader)

        (0...result.timesteps).each do |timestep|
          STDERR.puts "\e[33m timestep: #{timestep} ===================================================== \e[0m"

          # this works only for localhost
          #puts `docker exec cassandra_migrate nodetool flush`

          if timestep < result.timesteps - 1
            under_creating_indexes = result.time_depend_indexes.indexes_all_timestep[timestep + 1].indexes.to_set -
                result.time_depend_indexes.indexes_all_timestep[timestep].indexes.to_set

            under_creating_indexes.each do |under_creating_index|
              backend.create_index(under_creating_index, !options[:dry_run], true)
              STDERR.puts under_creating_index.key + " is created before query processing"
            end
          end

          migrator = Migrator::Migrator.new(backend, loader, options[:loader], options[:validate_migration])
          #migration_worker, _ = migrate_async(result, timestep, migrator)
          migrator.migrate(result, timestep)

          indexes_for_this_timestep = result.indexes_used_in_plans(timestep)
          index_values = index_values indexes_for_this_timestep, backend,
                                      options[:num_iterations],
                                      options[:fail_on_empty]
          group_tables = Hash.new { |h, k| h[k] = [] }
          group_totals = Hash.new { |h, k| h[k] = [0] * options[:num_iterations]}

          result.time_depend_plans.map{|tdp| tdp.plans[timestep]}.each do |plan|
            query = plan.query
            weight = result.workload.time_depend_statement_weights[query]
            next if query.is_a?(SupportQuery) || !weight
            @logger.debug { "Executing #{query.text}" }
            STDERR.puts "Executing Query: #{query.text}"
            STDERR.puts "    Executing Plan: #{plan.inspect}"

            next unless options[:group].nil? || plan.group == options[:group]

            measurement = nil
            #StackProf.run(mode: :wall, raw: true, out: "tmp/bench_query_#{timestep}.dump") do
              measurement = bench_query backend, plan.indexes, plan, index_values,
                                        options[:num_iterations],
                                        weight: weight
            #end
            next if measurement.empty?

            measurement.estimate = plan.cost
            group_totals[plan.group] = [group_totals[plan.group], measurement.values].transpose.map(&:sum)
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

              # re-setting parameters for the update.
              # index_values possible become obsolete because of the value on the CF can be changed by other update plans
              index_values = index_values indexes, backend,
                                          options[:num_iterations],
                                          options[:fail_on_empty],
                                          nullable_indexes: under_creating_indexes

              measurement = bench_update backend, indexes, plan, index_values,
                                         options[:num_iterations], weight: weight
              if measurement.empty?
                puts "measurement was empty"
                next
              end

              measurement.estimate = plan.cost
              group_totals[plan.group] = [group_totals[plan.group], measurement.values].transpose.map(&:sum)
              group_tables[plan.group] << measurement
            end
          end

          total = [0] * options[:num_iterations]
          table = []
          group_totals.each do |group, group_total|
            total = [total, group_total].transpose.map(&:sum)
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

          #migrator.stop
          migrator.exec_cleanup(result, timestep)
          GC.start
        end
      end

      private

      def setup_db(result, backend, loader)
        indexes = result.time_depend_indexes.indexes_all_timestep.first.indexes
        # Produce the DDL and execute unless the dry run option was given
        backend.create_indexes(indexes, !options[:dry_run], options[:skip_existing],
                               options[:drop_existing]).each {|ddl| STDERR.puts ddl}

        load_started = Time.now.utc
        # Create a new instance of the loader class and execute
        loader.load indexes, options[:loader], options[:progress],
                    options[:limit], options[:skip_nonempty]
        load_ended = Time.now.utc
        STDERR.puts "whole loading time: " + (load_ended - load_started).to_s
      end

      # Output the table of results
      # @return [void]
      def td_output_table(table)
        columns = [
            'timestep', 'label', 'group',
            { 'measurements.name' => { display_name: 'name' } },
            { 'measurements.weight' => { display_name: 'weight' } },
            { 'measurements.mean' => { display_name: 'mean' } },
            { 'measurements.estimate' => { display_name: 'cost' } },
            { 'measurements.standard_error' => { display_name: 'standard_error' } }
        ]

        tp table, *columns
      end

      # Output a CSV file of results
      # @return [void]
      def td_output_csv(table)
        csv_str = CSV.generate do |csv|
          csv << %w(timestep label group name weight mean cost standard_error)

          table.each do |group|
            group.measurements.each do |measurement|
              csv << [
                  group.timestep,
                  group.label,
                  group.group,
                  measurement.name,
                  measurement.weight,
                  measurement.mean,
                  measurement.estimate,
                  measurement.standard_error
              ]
            end
          end
        end

        puts csv_str
      end
    end
  end
end
