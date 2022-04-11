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

      option :search_objective, type: :string, default: 'timestep',
             enum: %w(timestep subset simplify),
             desc: 'define objective'
      option :max_space, type: :numeric, default: Float::INFINITY,
             aliases: '-s',
             desc: 'maximum space allocated to indexes'
      option :creation_cost, type: :numeric, default: 0.00001,
             aliases: '-c',
             desc: 'creation cost coefficient of column family'
      option :objective, type: :string, default: 'cost',
             enum: %w(cost space indexes),
             desc: 'the objective function to use in the ILP'
      option :start_size, type: :numeric, default: 3,
             desc: 'size of queries in subset workloads as start point'
      option :end_size, type: :numeric, default: 20,
             desc: 'size of queries in subset workloads as end point'
      option :each_try, type: :numeric, default: 500,
             desc: 'number of tries for each size of subset workload'

      def search_pattern(name)
        workload = Workload.load name

        # Prepare the workload and the cost model
        workload.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && workload.mix != :default
        cost_model = get_class_from_config options, 'cost', :cost_model

        if options[:search_objective] == 'timestep'
          search_timesteps workload, cost_model
        elsif options[:search_objective] == 'subset'
          search_subsets workload, cost_model
        else
          search_simplified_query workload, cost_model
        end
      end

      private

      def search_subsets(workload, cost_model)
        (@options[:start_size]..@options[:end_size]).each do |size|

          puts "============ calculation size: #{size} ======================================================================="
          statement_key_sets = (0...options[:each_try]).map do |_|
            workload.statement_weights.keys.reject{|q| q.is_a? Insert}.sample(size).to_set
          end.uniq

          puts "------------ statement_case_size: #{statement_key_sets.size} ----------------"

          Parallel.each(statement_key_sets, in_processes: [Parallel.processor_count - 2, 2].max()) do |statement_keys|
            #statement_key_sets.each do |statement_keys|

            queries = workload.statement_weights.select{|k, _| statement_keys.include? k}.keys

            puts "$$$$$$$$ #{queries.map{|q| q.comment}.join(', ')}"

            if workload.is_a? TimeDependWorkload
              sub_workload = TimeDependWorkload.new do |_|
                Model 'tpch'
                TimeSteps 3
              end
              #sub_workload.time_depend_statement_weights = queries
              queries.each {|query| sub_workload.add_statement query, frequency: [1,2,3]}
            else
              sub_workload = Workload.new {|_| Model 'tpch'}
              queries.each {|query| sub_workload.add_statement query.dup}
              workload.statement_weights.keys.select{|q| q.is_a? Insert}.each do |update|
                sub_workload.add_statement update.dup
              end
            end

            # Execute the advisor
            objective = Search::Objective.const_get options[:objective].upcase
            begin
              search_result sub_workload, cost_model, options[:max_space],
                            objective, false
            rescue InvalidIndexException => e
              puts "\n\n= subset queries ============================================================================================================="
              puts "statement size: #{size}"
              puts "exception: #{e.inspect}"
              puts "exception: #{e.backtrace.join("\n")}"
              puts sub_workload.statement_weights.inspect
              #puts sub_workload.statement_weights.map(&:text).join("\n")
              puts "==============================================================================================================\n\n"
            end
          end
        end
      end

      #    def search_subsets(start_size, end_size, workload, cost_model)
      #      is_subset_found = false
      #      (start_size..[end_size, workload.statement_weights.keys.size].min).to_a.reverse.each do |size|
      #        puts "============ calculation size: #{size} ======================================================================="
      #        statement_key_sets = (0...options[:each_try]).map do |_|
      #          workload.statement_weights.keys.sample(size).to_set
      #        end.uniq

      #        statement_key_sets = statement_key_sets.select{|sks| sks.any?{|s| s.comment == " -- Q2_outer"}}

      #        puts "------------ statement_case_size: #{statement_key_sets.size} ----------------"

      #        Parallel.each(statement_key_sets, in_processes: [Parallel.processor_count / 2, 2].max()) do |statement_keys|
      #          if is_subset_found
      #            puts "subset is already found in other cases. exit.."
      #            return
      #          end

      #          queries = workload.statement_weights.select{|k, _| statement_keys.include? k}.keys
      #          if workload.is_a? TimeDependWorkload
      #            sub_workload = TimeDependWorkload.new {|_| Model 'tpch'}
      #            sub_workload.time_depend_statement_weights = queries
      #          else
      #            sub_workload = Workload.new {|_| Model 'tpch'}
      #            queries.each {|query| sub_workload.add_statement query}
      #          end

      #          # Execute the advisor
      #          objective = Search::Objective.const_get options[:objective].upcase
      #          begin
      #            search_result sub_workload, cost_model, options[:max_space],
      #                          objective, false
      #          rescue Exception => e
      #            puts "\n\n= subset queries ============================================================================================================="
      #            puts "statement size: #{size}"
      #            puts "exception: #{e.inspect}"
      #            puts "exception: #{e.backtrace.join("\n")}"
      #            puts sub_workload.statement_weights.keys.map(&:text).join("\n")
      #            puts "==============================================================================================================\n\n"
      #            return if is_subset_found
      #            is_subset_found = true

      #            search_subsets(start_size, end_size, sub_workload, cost_model)
      #            return
      #          end
      #        end
      #      end
      #    end

      def search_simplified_query(workload, cost_model)
        simplified_queries = workload.statement_weights.keys.map{|q| q.simplified_queries}.reject(&:empty?)
        query_sets = simplified_queries.first
        simplified_queries[1..].each do |sqs|
          query_sets = query_sets.product(sqs).take(options[:each_try])
          query_sets.map!{|q| q.flatten!} if query_sets.first.first.is_a? Array
        end
        Parallel.each(query_sets, in_processes: Parallel.processor_count - 5) do |queries|
          #query_sets.each do |queries|
          if workload.is_a? TimeDependWorkload
            sub_workload = TimeDependWorkload.new {|_| Model 'tpch'}
          else
            sub_workload = Workload.new {|_| Model 'tpch'}
          end
          queries.each {|query| sub_workload.add_statement query.unparse}

          # Execute the advisor
          objective = Search::Objective.const_get options[:objective].upcase
          begin
            search_result sub_workload, cost_model, options[:max_space],
                          objective, false
          rescue Exception => e
            puts "\n\n= simplified queries ============================================================================================================="
            puts "exception: #{e.inspect}"
            puts "exception: #{e.backtrace.join("\n")}"
            puts sub_workload.statement_weights.keys.map(&:text).join("\n")
            puts "==============================================================================================================\n\n"

            search_simplified_query(sub_workload, cost_model)
            return
          end
        end
      end

      def search_timesteps(workload, cost_model)
        Parallel.each((1..workload.timesteps).to_a, in_processes: Parallel.processor_count - 3) do |timestep_size|
          workload_tmp = workload.dup
          workload_tmp.timesteps = timestep_size
          statements = workload_tmp.statement_weights.map do |statement, value|
            Hash[statement, value[0, timestep_size]]
          end.reduce(&:merge)
          workload_tmp.time_depend_statement_weights = statements

          # Execute the advisor
          objective = Search::Objective.const_get options[:objective].upcase
          begin
            search_result workload_tmp, cost_model, options[:max_space],
                          objective, false
            puts "searching is done for #{workload_tmp.timesteps}"
          rescue Exception => e
            puts "=================================================="
            puts "timestep_size: #{timestep_size}"
            puts "exception: #{e.inspect}"
            puts "exception: #{e.backtrace.join("\n")}"
            puts workload_tmp.statement_weights.keys.map(&:text).join("\n")
            puts "=================================================="
          end
        end
      end
    end
  end
end
