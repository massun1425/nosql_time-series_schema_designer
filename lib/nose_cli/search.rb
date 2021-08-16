# frozen_string_literal: true

require 'formatador'
require 'ostruct'
require 'json'
require 'stackprof'

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

      def search(name)
        # Get the workload from file or name
        if File.exist? name
          result = load_results name, options[:mix]
          workload = result.workload
        else
          workload = Workload.load name
        end

        RunningTimeLogger.info(RunningTimeLogger::Headers::START_RUNNING)
        workload = amplify_workload(workload) if options[:amplify]
        # Prepare the workload and the cost model
        workload.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && workload.mix != :default
        workload.remove_updates if options[:read_only]
        cost_model = get_class_from_config options, 'cost', :cost_model

        # Execute the advisor
        objective = Search::Objective.const_get options[:objective].upcase
        stating = Time.new
        StackProf.run(mode: :wall, raw: true, out: 'tmp/search_rubis.dump') do
          result = search_result workload, cost_model, options[:max_space],
                                 objective, options[:by_id_graph]
        end
        ending = Time.new
        puts "whole execution time: #{ending - stating}"
        output_search_result result, options unless result.nil?
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

      def amplify_workload(workload)
        queries = workload.statement_weights.keys.select{|s| s.is_a? Query}
        queries.each do |base_query|
          puts base_query.text
          amplified_queries = amplify_query base_query
          next if amplified_queries.nil?
          amplified_queries.each do |aq|
            puts "    " + aq.text
            frequency = workload.time_depend_statement_weights[base_query]
            workload.add_statement aq.text,
                                   {workload.mix => frequency},
                                   group: base_query.group,
                                   frequency: frequency
          end
        end

        workload
      end

      private

      def amplify_query(query)
        return if query.graph.entities.size > 1
        return if query.conditions.values.select{|c| c.operator == "=".to_sym}.size > 1
        entity = query.graph.entities.first
        entity
        amplify_queries = entity.fields.values.delete_if{|f| f == query.conditions.values.first.field}.map do |f|
          tmp_query = query.dup
          tmp_query.conditions = {f.id => Condition.new(f, "=".to_sym, nil)}
          tmp_query.comment = tmp_query.comment + "_amplified_#{f.id}"
          tmp_query.set_text
          tmp_query
        end
        amplify_queries
      end
    end
  end
end
