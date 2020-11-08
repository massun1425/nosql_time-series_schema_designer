# frozen_string_literal: true

require 'formatador'
require 'ostruct'
require 'json'

module NoSE
  module CLI
    # Add a command to run the advisor for a given workload
    class NoSECLI < Thor
      desc 'search NAME', 'run the workload NAME'

      long_desc <<-LONGDESC
        `nose search` is the primary command to run the NoSE advisor. It will
        load the given workload, enumerate indexes for each statement, and
        construct and solve an ILP to produce statement execution plans.
      LONGDESC

      shared_option :mix
      shared_option :format
      shared_option :output

      option :max_space, type: :numeric, default: Float::INFINITY,
             aliases: '-s',
             desc: 'maximum space allocated to indexes'
      option :enumerated, type: :boolean, default: false, aliases: '-e',
             desc: 'whether enumerated indexes should be output'
      option :read_only, type: :boolean, default: false,
             desc: 'whether to ignore update statements'
      option :objective, type: :string, default: 'cost',
             enum: %w(cost space indexes),
             desc: 'the objective function to use in the ILP'
      option :by_id_graph, type: :boolean, default: false,
             desc: 'whether to group generated indexes in' \
                                 'graphs by ID',
             aliases: '-i'

      def search_migrations(name)
        # Get the workload from file or name
        if File.exist? name
          result = load_results name, options[:mix]
          workload = result.workload
        else
          workload = Workload.load name
        end

        ENV['TZ'] = 'Asia/Tokyo'
        started_time = Time.now.to_s.gsub(" ", "_")

        # Prepare the workload and the cost model
        workload.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && workload.mix != :default
        workload.remove_updates if options[:read_only]
        cost_model = get_class_from_config options, 'cost', :cost_model

        # Execute the advisor
        objective = Search::Objective.const_get options[:objective].upcase

        dir_name = "search_migration_result/" + + "#{name}_#{started_time}".gsub("/", "_").gsub(" ", "")
        FileUtils.mkdir_p(dir_name)
        File.open("#{dir_name}/workload.rb", "w") do |f|
          f.puts workload.source_code
        end

        enumerated_indexes = enumerate_indexes(workload, cost_model)

        search = Search::CachedSearch.new(workload, cost_model, objective, options[:by_id_graph], options[:prunedCF])
        enumerated_indexes = search.pruning_indexes_by_plan_cost enumerated_indexes
        query_trees = search.get_query_trees_hash enumerated_indexes
        support_query_trees = search.get_support_query_trees_hash query_trees, enumerated_indexes

        creation_coefs = [1.0e-06, 1.0e-09]
        migrate_sup_coefs = [1.0e-15, 1.0e-20, 1.0e-25]
        intervals = [3000, 7000]
        index_creation_time_coefs = [1.0e-06, 1.0e-09, 1.0e-12] # yusuke これと creation_coef は１つに統合できるのでは?

        parameter_combinations = creation_coefs.product(migrate_sup_coefs, intervals, index_creation_time_coefs)
        #Parallel.each(parameter_combinations.reverse, in_processes: Etc.nprocessors / 2) do |(creation_coe, migrate_sup_coe, interval, index_creation_time_coe)|
        parameter_combinations.reverse.each do |(creation_coe, migrate_sup_coe, interval, index_creation_time_coe)|
          puts "======================================================================================="
          puts creation_coe, migrate_sup_coe, interval, index_creation_time_coe
          puts "======================================================================================="

          workload.creation_coeff = creation_coe
          workload.migrate_support_coeff = migrate_sup_coe
          workload.reset_interval interval
          ENV['INDEX_CREATION_TIME_COEFF'] = index_creation_time_coe.to_s

          result = search.search_overlap enumerated_indexes, query_trees, support_query_trees

          if result.migrate_plans.size >= 1
            File.open(dir_name + "/" + [creation_coe, migrate_sup_coe, interval, index_creation_time_coe, result.migrate_plans.size].join("-").gsub(".", "_") + ".txt", "w") do |f|
              f.puts "======================================================================================="
              f.puts "creation_coe, migrate_sup_coe, interval, index_creation_time_coeff, migration plan num"
              f.puts creation_coe, migrate_sup_coe, interval, index_creation_time_coe, result.migrate_plans.size.to_s
              output_migration_plans_txt result.migrate_plans, f, 1
              f.puts "======================================================================================="
            end
            File.open(dir_name + "/" + "mig_plan_size_" + [creation_coe, migrate_sup_coe, interval, index_creation_time_coe, result.migrate_plans.size].join("-").gsub(".", "_") + "_whole_result_" + ".txt", "w") do |f|
              output_json result, f
              output_txt result, f
            end
          end
        end
      end
    end
  end
end
