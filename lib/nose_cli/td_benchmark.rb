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

      shared_option :mix

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

          indexes = result.time_depend_indexes.indexes_all_timestep[timestep].indexes
          #index_values = index_values indexes, backend,
          #                            options[:num_iterations],
          #                            options[:fail_on_empty]
          #group_tables = Hash.new { |h, k| h[k] = [] }
          #group_totals = Hash.new { |h, k| h[k] = 0 }

          break if timestep == result.timesteps - 1
          exec_migration(result, backend, timestep)


          sleep(1)
        end
      end

      private

      def set_up_db(result, backend, loader)
        indexes = result.time_depend_indexes.indexes_all_timestep.first.indexes
        # Produce the DDL and execute unless the dry run option was given
        backend.create_time_depend_indexes(indexes, !options[:dry_run], options[:skip_existing],
                                           options[:drop_existing])

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
            Enumerator.new do |enum|
              backend.create_index(new_index, enum, !options[:dry_run], options[:skip_existing])
            end.each {|ddl| puts ddl}
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
