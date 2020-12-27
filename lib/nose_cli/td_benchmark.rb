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

          migration_worker, _ = exec_migration_async(plan_file, result, timestep)
          #exec_migration(plan_file, result, timestep)

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

            measurement = nil
            #StackProf.run(mode: :wall, raw: true, out: "tmp/bench_query_#{timestep}.dump") do
              measurement = bench_query backend, plan.indexes, plan, index_values,
                                        options[:num_iterations],
                                        weight: weight
            #end
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
          exec_cleanup(backend, result, timestep)
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

      def exec_migration(plan_file, result, timestep)
        migration_plans = result.migrate_plans.select{|mp| mp.start_time == timestep}

        #Parallel.each(migration_plans, in_threads: 10) do |migration_plan|
        migration_plans.each do |migration_plan|
          _, backend = load_time_depend_plans plan_file, options
          loader = get_class('loader', options).new result.workload, backend
          prepare_next_indexes(migration_plan, backend, loader)
        end
      end

      def left_outer_join(backend, left_index, left_values, right_index, right_values)
        overlap_fields = (left_index.all_fields & right_index.all_fields).to_a
        right_index_hash = {}

        starting = Time.now
        # create hash for right values
        right_values.each do |right_value|
          next if backend.remove_null_place_holder_row([right_value]).empty?

          key_fields = overlap_fields.select{|f| f.is_a? NoSE::Fields::IDField}.map{|fi| right_value.slice(fi.id)}
          next if backend.remove_null_place_holder_row(key_fields).empty?

          key_fields = overlap_fields.select{|f| f.is_a? NoSE::Fields::IDField}.map{|fi| right_value[fi.id].to_s}.join(',')
          key_fields = Zlib.crc32(key_fields)
          if right_index_hash.has_key?(key_fields)
            right_index_hash[key_fields] << right_value
          else
            right_index_hash[key_fields] = [right_value]
          end
        end
        puts "left outer join hash creation done: #{Time.now - starting}"

        results = []
        # iterate for left value to look for checking does related record exist
        left_values.each do |left_value|
          related_key = overlap_fields.select{|f| f.is_a? NoSE::Fields::IDField}.map{|fi| left_value[fi.id].to_s}.join(',')
          related_key = Zlib.crc32(related_key)
          if right_index_hash.has_key?(related_key)
            right_index_hash[related_key].each do |right_value|
              results << left_value.merge(right_value)
            end
          else
            results << left_value.merge(backend.create_empty_record(right_index))
          end
        end.compact
        puts "hash join done #{Time.now - starting}"
        results
      end

      def full_outer_join(backend, index_values)
        return index_values.to_a.flatten(1)[1] if index_values.length == 1

        result = []
        index_values.each_cons(2) do |former_index_value, next_index_value|
          puts "former index #{former_index_value[0].key} has #{former_index_value[1].size} records"
          puts "next index #{next_index_value[0].key} has #{next_index_value[1].size} records"
          result += left_outer_join(backend, former_index_value[0], former_index_value[1], next_index_value[0], next_index_value[1])
          result += left_outer_join(backend, next_index_value[0], next_index_value[1], former_index_value[0], former_index_value[1])
          result.uniq!
        end
        result
      end

      # @param [MigratePlan, Backend]
      def prepare_next_indexes(migrate_plan, backend, loader)
        STDERR.puts "\e[36m migrate from: \e[0m"
        migrate_plan.obsolete_plan&.map{|step| STDERR.puts '  ' + step.inspect}
        STDERR.puts "\e[36m to: \e[0m"
        migrate_plan.new_plan.map{|step| STDERR.puts '  ' + step.inspect}

        migrate_plan.new_plan.steps.each do |new_step|
          next unless new_step.is_a? Plans::IndexLookupPlanStep
          query_plan = migrate_plan.prepare_plans.find{|pp| pp.index == new_step.index}&.query_plan
          next if query_plan.nil?

          target_index = new_step.index
          values = index_records(query_plan.indexes, backend, target_index.all_fields)
          obsolete_data = full_outer_join(backend, values)

          STDERR.puts "===== creating index: #{target_index.key} for the migration"
          unless backend.index_exists?(target_index)
            STDERR.puts backend.create_index(target_index, !options[:dry_run], options[:skip_existing])
          end
          STDERR.puts "collected data size for #{target_index.key} is #{obsolete_data.size}"
          backend.load_index_by_COPY(target_index, obsolete_data)
          STDERR.puts "===== creation done: #{target_index.key} for the migration"

          validate_migration_process(loader, target_index) #if ENV['BENCH_MODE'] == 'debug'
        end
      end

      def validate_migration_process(loader, new_index)
        STDERR.puts "validating migration process for #{new_index.key}"
        loader.load_dummy [new_index], options[:loader], options[:progress],
                    options[:limit], options[:skip_nonempty]
      end

      def exec_cleanup(backend, result, timestep)
        STDERR.puts "cleanup"
        migration_plans = result.migrate_plans.select{|mp| mp.start_time == timestep}

        return if timestep + 1 == result.timesteps
        next_ts_indexes = result.time_depend_indexes.indexes_all_timestep[timestep + 1].indexes
        drop_obsolete_tables(migration_plans, backend, next_ts_indexes)
      end

      def index_records(indexes, backend, required_fields)
        Hash[indexes.map do |index|
          values = backend.index_records(index, required_fields).to_a
          [index, values]
        end]
      end

      def drop_obsolete_tables(migrate_plans, backend, next_ts_indexes)
        obsolete_indexes = migrate_plans.flat_map do |mp|
          next if mp.obsolete_plan.nil?
          mp.obsolete_plan.indexes.select {|index| not next_ts_indexes.include?(index)}
        end.uniq
        obsolete_indexes.each do |obsolete_index|
          STDERR.puts "drop CF: #{obsolete_index.key}"
          backend.drop_index(obsolete_index)
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
