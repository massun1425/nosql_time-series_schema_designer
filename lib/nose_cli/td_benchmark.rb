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
      option :num_iterations, type: :numeric, default: 10,
             banner: 'ITERATIONS',
             desc: 'the number of times to execute each ' \
                                    'statement'
      option :group, type: :string, default: nil, aliases: '-g',
             desc: 'restrict the benchmark to statements in the ' \
                           'given group'
      option :fail_on_empty, type: :boolean, default: true,
             desc: 'abort if a column family is empty'
      option :totals, type: :boolean, default: true, aliases: '-t',
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
      option :validate_migration, type: :boolean, default: false, aliases: '-v',
             desc: 'whether validate migration process'
      option :migrate_async, type: :boolean, default: true,
             desc: 'whether migrate async'
      option :ideal, type: :boolean, default: false,
             desc: 'whether benchmark ideal schemas'

      def td_benchmark(plan_file)
        label = File.basename plan_file, '.*'
        start_benchmarking = Time.now

        result, backend = load_time_depend_plans plan_file, options
        backend.initialize_client
        loader = get_class('loader', options).new result.workload, backend
        backend.clear_keyspace
        index_values = setup_db(result, backend, loader, options[:ideal])
        migrator = Migrator::Migrator.new(backend, loader, result, options[:loader], options[:validate_migration])

          (0...result.timesteps).each do |timestep|
            STDERR.puts "\e[33m timestep: #{timestep} at #{Time.now.utc} ===================================================== \e[0m"
            migrator.create_next_indexes timestep
            options[:migrate_async] ?
              migrator.migrate_async(timestep, options[:migrate_async])
              : migrator.migrate(timestep, options[:migrate_async])

            indexes_for_this_timestep = result.indexes_used_in_plans(timestep)
            indexes_for_this_timestep.each do |index_for_this_timestep|
              raise "used index is not loaded: #{index_for_this_timestep.key}" if backend.index_empty? index_for_this_timestep
            end

            not_collected_indexes = indexes_for_this_timestep.select{|i| not index_values.has_key?(i)}
            index_values.merge!(index_values_by_mysql(not_collected_indexes, backend, loader, options[:loader], options[:num_iterations]))

            group_tables = Hash.new { |h, k| h[k] = [] }
            group_totals = Hash.new { |h, k| h[k] = [0] * options[:num_iterations]}

            result.time_depend_plans.map{|tdp| tdp.plans[timestep]}.each do |plan|
              current_backend = get_backend options.dup, result.dup
              query = plan.query
              weight = result.workload.time_depend_statement_weights[query]
              next if query.is_a?(SupportQuery) || !weight
              @logger.debug { "Executing #{query.text}" }
              STDERR.puts "Executing Query: #{query.text}"
              STDERR.puts "    Executing Plan: #{plan.inspect}"

              next unless options[:group].nil? || plan.group == options[:group]

              STDERR.puts "start benchmarking Query"
              measurement = bench_query current_backend, plan.indexes, plan, index_values,
                                        options[:num_iterations],
                                        weight: weight
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

                STDERR.puts "collect index_values for UPDATE"
                # re-setting parameters for the update.
                # index_values possible become obsolete because of the value on the CF can be changed by other update plans
                index_values = index_values indexes, backend,
                                            options[:num_iterations],
                                            options[:fail_on_empty],
                                            nullable_indexes: migrator.get_under_constructing_indexes(timestep)

                STDERR.puts "start benchmarking UPDATES"
                STDERR.puts plan.inspect
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

            migrator.stop if options[:migrate_async]
            migrator.exec_cleanup(timestep) unless options[:ideal]
            GC.start
          end

        puts "whole benchmarking time #{Time.now - start_benchmarking}"
      end

      private

      def setup_db(result, backend, loader, is_ideal = false)
        # create all indexes at once before first timestep for ideal schemas
        indexes = is_ideal ? result.time_depend_indexes.indexes_all_timestep.flat_map(&:indexes).uniq
                    : result.time_depend_indexes.indexes_all_timestep.first.indexes
        # Produce the DDL and execute unless the dry run option was given
        backend.create_indexes(indexes, !options[:dry_run], options[:skip_existing],
                               options[:drop_existing]).each {|ddl| STDERR.puts ddl}

        load_started = Time.now.utc
        # Create a new instance of the loader class and execute
        index_values = loader.load indexes, options[:loader], options[:progress],
                                   options[:limit], options[:skip_nonempty], options[:num_iterations]
        load_ended = Time.now.utc
        STDERR.puts "whole loading time: " + (load_ended - load_started).to_s
        index_values.map do |i, records|
          records = Backend::CassandraBackend.remove_any_null_place_holder_row records
          Hash[i, backend.cast_records(i, records)]
        end.reduce(&:merge)
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
          csv << %w(timestep label group name weight mean cost standard_error middle_mean values)

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
                measurement.standard_error,
                measurement.middle_mean,
                measurement.values
              ]
            end
          end
        end

        puts csv_str
      end
    end
  end
end
