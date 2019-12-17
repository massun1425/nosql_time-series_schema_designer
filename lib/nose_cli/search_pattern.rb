# frozen_string_literal: true

require 'formatador'
require 'ostruct'
require 'json'
require 'parallel'
require 'etc'

module NoSE
  module CLI
    # Add a command to run the advisor for a given workload
    class NoSECLI < Thor
      desc 'search_pattern NAME', 'run the subsets of NAME'

      long_desc <<-LONGDESC
        `nose search_pattern` is a command for debugging.
        This command executes search command for subset workloads of given workload.
      LONGDESC

      shared_option :mix
      shared_option :format
      shared_option :output

      option :max_space, type: :numeric, default: Float::INFINITY,
                         aliases: '-s',
                         desc: 'maximum space allocated to indexes'
      option :creation_cost, type: :numeric, default: 0.00001,
             aliases: '-c',
             desc: 'creation cost coefficient of column family'
      option :read_only, type: :boolean, default: false,
                         desc: 'whether to ignore update statements'
      option :objective, type: :string, default: 'cost',
                         enum: %w(cost space indexes),
                         desc: 'the objective function to use in the ILP'
      option :by_id_graph, type: :boolean, default: false,
                           desc: 'whether to group generated indexes in' \
                                 'graphs by ID',
                           aliases: '-i'
      option :start_size, type: :numeric, default: 5,
                            desc: 'size of queries in subset workloads as start point'
      option :end_size, type: :numeric, default: 10,
             desc: 'size of queries in subset workloads as end point'
      option :each_try, type: :numeric, default: 40,
             desc: 'number of tries for each size of subset workload'

      def search_pattern(name)
        workload = Workload.load name

        # Prepare the workload and the cost model
        workload.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && workload.mix != :default
        workload.remove_updates if options[:read_only]
        cost_model = get_class_from_config options, 'cost', :cost_model

        (options[:start_size]...options[:end_size]).each do |size|
          Parallel.each(workload.statement_weights.keys.combination(size).to_a[0..options[:each_try]], in_threads: Etc.nprocessors - 1) do |statement_keys|
            workload_tmp = workload.dup
            statements = workload_tmp.statement_weights.select{|k, _| statement_keys.include? k}
            workload_tmp.time_depend_statement_weights = statements

            # Execute the advisor
            objective = Search::Objective.const_get options[:objective].upcase
            begin
              search_result workload_tmp, cost_model, options[:max_space],
                                     objective, options[:by_id_graph]
            rescue Exception => e
              puts "#{size}"
              puts "exception: #{e}"
              puts workload_tmp.statement_weights.inspect
            end
          end
        end
      end
    end
  end
end
