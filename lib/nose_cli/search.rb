# frozen_string_literal: true

require 'formatador'
require 'ostruct'
require 'json'
require 'stackprof'
require 'sigdump/setup'

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
      option :frequency_type, type: :string,
             enum: %w(time_depend static firstTs lastTs ideal),
             desc: 'choose frequency type of workload'

      def search(name, max_space = nil)
        # Get the workload from file or name
        if File.exist? name
          result = load_results name, options[:mix]
          workload = result.workload
        else
          workload = Workload.load name
        end

        RunningTimeLogger.info(RunningTimeLogger::Headers::START_RUNNING)
        # Prepare the workload and the cost model
        workload.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && workload.mix != :default
        workload.remove_updates if options[:read_only]
        workload.set_frequency_type options[:frequency_type] unless options[:frequency_type].nil?
        cost_model = get_class_from_config options, 'cost', :cost_model

        # Execute the advisor
        objective = Search::Objective.const_get options[:objective].upcase
        stating = Time.new
        #StackProf.run(mode: :cpu, out: 'stackprof_search.dump', raw: true) do
        result = search_result workload, cost_model, max_space.nil? ? options[:max_space] : max_space,
                               objective, options[:by_id_graph]
        #end
        ending = Time.new
        puts "whole execution time: #{ending - stating}"
        RunningTimeLogger.info(RunningTimeLogger::Headers::END_RUNNING)
        RunningTimeLogger.write_running_times

        print_each_plan_cost result
        output_search_result result, options unless result.nil?
        result
      end

      private

      def print_each_plan_cost(result)
        return unless @workload.instance_of?(TimeDependWorkload)
        puts "=============================="
        result.time_depend_plans.sort_by{|tp| tp.query.comment}.each do |tp|
          puts "#{tp.query.comment}, #{tp.plans.map{|p| p.steps
                                                         .select{|s| s.instance_of? Plans::IndexLookupPlanStep}.size > 1 ? "JP" : "MV"}
                                         .zip(tp.plans.map(&:cost))}"
        end
        puts "=============================="
      end

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
          send(('output_txt').to_sym,
               result, file, options[:enumerated], backend)
          send(('output_json').to_sym,
               result, file, options[:enumerated], backend)
        ensure
          file.close unless options[:output].nil?
        end
      end

    end
  end
end
