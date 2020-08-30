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

        # Prepare the workload and the cost model
        workload.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && workload.mix != :default
        workload.remove_updates if options[:read_only]
        cost_model = get_class_from_config options, 'cost', :cost_model

        # Execute the advisor
        objective = Search::Objective.const_get options[:objective].upcase


        @creation_coeff = 0.000_000_001 # Hyper Parameter
        @migrate_support_coeff = 0.000_000_1 # Hyper Parameter
        [0.000_000_000_1, 0.000_000_001, 0.000_000_01].each do |creation_coe|
          [0.000_000_01, 0.000_000_1, 0.000_001].each do |migrate_sup_coe|
            [100, 3000].each do |interval|
              workload.creation_coeff = creation_coe
              workload.migrate_support_coeff = migrate_sup_coe
              workload.interval = interval

              result = search_result workload, cost_model, options[:max_space],
                                     objective, options[:by_id_graph]
              #if result.migrate_plans.size >= 3
                puts "======================================================================================="
                puts creation_coe, migrate_sup_coe, interval, result.migrate_plans.size
                puts "======================================================================================="
              #end
            end
          end
        end
      end

      private

      # Output results from the search procedure
      # @return [void]
      def output_search_result(result, options)
        # Output the results in the specified format
        file = if options[:output].nil?
                 $stdout
               else
                 File.open(options[:output], 'w')
               end

        begin
          backend = get_backend options, result
          send(('output_' + options[:format]).to_sym,
               result, file, options[:enumerated], backend)
        ensure
          file.close unless options[:output].nil?
        end
      end
    end
  end
end
