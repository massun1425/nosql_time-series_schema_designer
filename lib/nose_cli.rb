# frozen_string_literal: true

require 'erb'
require 'formatador'
require 'parallel'
require 'thor'
require 'yaml'

require 'nose'
require_relative 'nose_cli/measurements'

module NoSE
  # CLI tools for running the advisor
  module CLI
    # A command-line interface to running the advisor tool
    class NoSECLI < Thor
      # The path to the configuration file in the working directory
      CONFIG_FILE_NAME = 'nose.yml'

      check_unknown_options!

      class_option :debug, type: :boolean, aliases: '-d',
                   desc: 'enable detailed debugging information'
      class_option :parallel, type: :boolean, default: true,
                   desc: 'run various operations in parallel'
      class_option :colour, type: :boolean, default: nil, aliases: '-c',
                   desc: 'enabled coloured output'
      class_option :interactive, type: :boolean, default: true,
                   desc: 'allow actions which require user input'
      class_option :prunedCF, type: :boolean, default: true,
                 desc: 'whether enumerate CFs using pruned index enumerator or not'
      class_option :enumerator, type: :string, default: 'graph_based',
                   enum: %w(default pruned simple graph_based),
                   desc: 'the objective function to use in the ILP'
      class_option :iterative, type: :boolean, default: true,
                   desc: 'whether execute optimization in iterative method'
      class_option :is_shared_field_threshold, type: :numeric, default: 2,
                   desc: 'query num threshold for decide whether the field is shared among queries'
      class_option :choice_limit, type: :numeric, default: 10_000,
                   desc: 'maximum number of key combinations for indexes in PrunedIndexEnumerator'

      def initialize(_options, local_options, config)
        super

        # Set up a logger for this command
        cmd_name = config[:current_command].name
        @logger = Logging.logger["nose::#{cmd_name}"]

        # Peek ahead into the options and prompt the user to create a config
        check_config_file interactive?(local_options)

        force_colour(options[:colour]) unless options[:colour].nil?

        # Disable parallel processing if desired
        Parallel.instance_variable_set(:@processor_count, 0) \
          unless options[:parallel]
      end

      private

      # Check if the user has disabled interaction
      # @return [Boolean]
      def interactive?(options = [])
        parse_options = self.class.class_options
        opts = Thor::Options.new(parse_options).parse(options)
        opts[:interactive]
      end

      # Check if the user has created a configuration file
      # @return [void]
      def check_config_file(interactive)
        return if File.file?(CONFIG_FILE_NAME)

        if interactive
          no_create = no? 'nose.yml is missing, ' \
                          'create from nose.yml.example? [Yn]'
          example_cfg = File.join Gem.loaded_specs['nose-cli'].full_gem_path,
                                  'data', 'nose-cli', 'nose.yml.example'
          FileUtils.cp example_cfg, CONFIG_FILE_NAME unless no_create
        else
          @logger.warn 'Configuration file missing'
        end
      end

      # Add the possibility to set defaults via configuration
      # @return [Thor::CoreExt::HashWithIndifferentAccess]
      def options
        original_options = super
        return original_options unless File.exist? CONFIG_FILE_NAME
        defaults = YAML.load_file(CONFIG_FILE_NAME).deep_symbolize_keys || {}
        Thor::CoreExt::HashWithIndifferentAccess \
          .new(defaults.merge(original_options))
      end

      # Get a backend instance for a given configuration and dataset
      # @return [Backend::Backend]
      def get_backend(config, result)
        be_class = get_class 'backend', config
        be_class.new result.workload.model, result.indexes,
                     result.plans, result.update_plans, config[:backend]
      end

      # Get a class of a particular name from the configuration
      # @return [Object]
      def get_class(class_name, config)
        name = config
        name = config[class_name.to_sym][:name] if config.is_a? Hash
        require "nose/#{class_name}/#{name}"
        name = name.split('_').map(&:capitalize).join
        full_class_name = ['NoSE', class_name.capitalize,
                           name + class_name.capitalize]
        full_class_name.reduce(Object) do |mod, name_part|
          mod.const_get name_part
        end
      end

      # Get a class given a set of options
      # @return [Object]
      def get_class_from_config(options, name, type)
        object_class = get_class name, options[type][:name]
        object_class.new(**options[type])
      end

      # Collect all advisor results for schema design problem
      # @return [Search::Results]
      def search_result(workload, cost_model, max_space = Float::INFINITY,
                        objective = Search::Objective::COST,
                        by_id_graph = false)

        STDERR.puts "measure runtime: start enumeration: #{DateTime.now.strftime('%Q')}"
        enumerated_indexes = enumerate_indexes(workload, cost_model)

        search = (options[:iterative] and workload.timesteps > 3) ?
                     Search::IterativeSearch.new(workload, cost_model, objective, by_id_graph, options[:prunedCF])
                     : Search::Search.new(workload, cost_model, objective, by_id_graph, options[:prunedCF])
        indexes = search.pruning_indexes_by_plan_cost enumerated_indexes
        search.search_overlap indexes, max_space
      end

      # enumerate indexes
      # @return [Search::Index]
      def enumerate_indexes(workload, cost_model)
        STDERR.puts "enumerate column families"
        if options[:enumerator] == "pruned"
          enumerated_indexes =
              PrunedIndexEnumerator.new(workload, cost_model,
                                        options[:is_shared_field_threshold], 2,2, choice_limit_size: options[:choice_limit]) \
                                            .indexes_for_workload.to_a
        elsif options[:enumerator] == "graph_based"
          enumerated_indexes =
            GraphBasedIndexEnumerator.new(workload, cost_model, 2, options[:choice_limit]) \
                                            .indexes_for_workload.to_a
        elsif options[:enumerator] == "default"
          enumerated_indexes = IndexEnumerator.new(workload) \
                                              .indexes_for_workload.to_a
        elsif options[:enumerator] == "simple"
          enumerated_indexes = SimpleIndexEnumerator.new(workload) \
                                              .indexes_for_workload.to_a
        end
        enumerated_indexes
      end

      # Load results of a previous search operation
      # @return [Search::Results]
      def load_results(plan_file, mix = 'default')
        representer = Serialize::SearchResultRepresenter.represent \
          Search::Results.new
        file = File.read(plan_file)

        case File.extname(plan_file)
        when '.json'
          result = representer.from_json(file)
        when '.rb'
          result = Search::Results.new
          workload = binding.eval file, plan_file
          result.instance_variable_set :@workload, workload
        end

        result.workload.mix = mix.to_sym unless \
          mix.nil? || (mix == 'default' && result.workload.mix != :default)

        result
      end

      # Load plans either from an explicit file or the name
      # of something in the plans/ directory
      def load_plans(plan_file, options)
        if File.exist? plan_file
          result = load_results(plan_file, options[:mix])
        else
          schema = Schema.load plan_file
          result = OpenStruct.new
          result.workload = Workload.new schema.model
          result.indexes = schema.indexes.values
        end
        backend = get_backend(options, result)

        [result, backend]
      end

      def load_time_depend_results(plan_file, mix = 'default')
        representer = Serialize::SearchTimeDependResultRepresenter.represent \
          Search::TimeDependResults.new
        file = File.read(plan_file)

        case File.extname(plan_file)
        when '.json'
          result = representer.from_json(file)
        when '.rb'
          result = Search::Results.new
          workload = binding.eval file, plan_file
          result.instance_variable_set :@workload, workload
        end

        result.workload.mix = mix.to_sym unless \
          mix.nil? || (mix == 'default' && result.workload.mix != :default)

        result
      end

      # Load plans either from an explicit file or the name
      # of something in the plans/ directory
      def load_time_depend_plans(plan_file, options)
        if File.exist? plan_file
          result = load_time_depend_results(plan_file, options[:mix])
        else
          schema = Schema.load plan_file
          result = OpenStruct.new
          result.workload = Workload.new schema.model
          result.indexes = schema.indexes.values
        end
        backend = get_backend(options, result)

        [result, backend]
      end

      # Output a list of indexes as text
      # @return [void]
      def time_depend_output_indexes_txt(header, indexes, file)
        file.puts Formatador.parse("[blue]#{header}[/]")
        indexes.each_with_index do |index_set, ts|
          file.puts Formatador.parse("[blue]for timestep: #{ts}[/]")
          index_set = [index_set] unless index_set.is_a?(Array) or index_set.is_a?(Set)
          index_set.sort_by(&:hash_str).each { |index| file.puts index.inspect }
        end
        file.puts
      end

      # Output a list of indexes as text
      # @return [void]
      def output_indexes_txt(header, indexes, file)
        file.puts Formatador.parse("[blue]#{header}[/]")
        indexes.sort_by(&:key).each { |index| file.puts index.inspect }
        file.puts
      end

      # Output a list of query plans as text
      # @return [void]
      def time_depend_output_plans_txt(plans, file, indent, weights)
        plans.each do |plan_for_all_timestep|
          file.puts Formatador.parse("[yellow]============= query: #{plan_for_all_timestep[0].query.text} ============[/]")
          plan_for_all_timestep.each_with_index do |plan, ts|
            file.puts Formatador.parse("[blue]for timestep: #{ts}[/]")
            weight = (plan.weight || weights[plan.query || plan.name])
            next if weight.nil?
            cost = plan.cost * weight[ts]

            file.puts "GROUP #{plan.group}" unless plan.group.nil?

            weight = " (cost:#{plan.cost}) * (weight: #{weight[ts]}) = #{cost}"
            file.puts '  ' * (indent - 1) + plan.query.label \
            unless plan.query.nil? || plan.query.label.nil?
            file.puts '  ' * (indent - 1) + plan.query.inspect + weight
            plan.each { |step| file.puts '  ' * indent + step.inspect }
            file.puts
          end
        end
      end

      # Output a list of query plans as text
      # @return [void]
      def output_plans_txt(plans, file, indent, weights)
        plans.each do |plan|
          weight = (plan.weight || weights[plan.query || plan.name])
          next if weight.nil?
          cost = plan.cost * weight

          file.puts "GROUP #{plan.group}" unless plan.group.nil?

          weight = " (cost:#{plan.cost}) * (weight: #{weight}) = #{cost}"
          file.puts '  ' * (indent - 1) + plan.query.label \
            unless plan.query.nil? || plan.query.label.nil?
          file.puts '  ' * (indent - 1) + plan.query.inspect + weight
          plan.each { |step| file.puts '  ' * indent + step.inspect }
          file.puts
        end
      end

      def output_migration_plans_txt(plans, file, indent)
        header = "Migrate plans\n" + '━' * 50
        file.puts Formatador.parse("[blue]#{header}[/]")
        #plans.sort_by{|mp| [mp.start_time, (mp.query.is_a?(Query) ? mp.query.text : mp.query)]}.each do |migrate_plan|
        plans.sort_by{|mp| [mp.query.text, mp.start_time]}.each do |migrate_plan|
          file.puts '  ' * (indent - 1) + (migrate_plan.query.is_a?(Query) ? migrate_plan.query.label : migrate_plan.query.text) \
            unless migrate_plan.query.nil? || (migrate_plan.query.is_a?(Query) ? migrate_plan.query.label.nil? : migrate_plan.query.nil?)
          file.puts '  ' * (indent - 1) + migrate_plan.query.inspect
          file.puts Formatador.parse('  ' * indent + "[blue]timestep: #{migrate_plan.start_time} to #{migrate_plan.end_time}[/]")
          file.puts Formatador.parse('  ' * indent + "[blue]obsolete plan, cost: #{migrate_plan.obsolete_plan&.cost} [/]")
          migrate_plan.obsolete_plan&.each { |step| file.puts '  ' * (indent + 1) + step.inspect }
          file.puts Formatador.parse('  ' * indent + "[blue]new plan, cost #{migrate_plan.new_plan.cost} [/]")
          migrate_plan.new_plan&.each { |step| file.puts '  ' * (indent + 1) + step.inspect }
          migrate_plan.prepare_plans.each do |prepare_plan|
            file.puts Formatador.parse('  ' * indent + "[blue]prepare plan, cost: #{prepare_plan.query_plan.cost}: for #{prepare_plan.index.inspect}[/]")
            prepare_plan.query_plan.each { |step| file.puts '  ' * (indent + 1) + step.inspect }
          end
          file.puts
        end
      end

      def output_plans_one_timestep_txt(plans, file, indent, weights, ts)
        plans.each do |target_plan|
          weight = (target_plan.weight || weights[target_plan.query || target_plan.name])
          next if weight.nil?
          cost = target_plan.cost * weight[ts]

          file.puts "GROUP #{target_plan.group}" unless target_plan.group.nil?

          weight = " * #{weight[ts]} = #{cost}"
          file.puts '  ' * (indent - 1) + target_plan.query.label \
          unless target_plan.query.nil? || target_plan.query.label.nil?
          file.puts '  ' * (indent - 1) + target_plan.query.inspect + weight
          target_plan.each { |step| file.puts '  ' * indent + step.inspect }
          file.puts
        end
      end

      # Output update plans as text
      # @return [void]
      def time_depend_output_update_plans_txt(update_plans, file, weights, mix = nil)
        return if update_plans.nil?
        unless update_plans.all?{ |update_plan| update_plan.empty?}
          header = "Update plans\n" + '━' * 50
          file.puts Formatador.parse("[blue]#{header}[/]")
        end

        update_plans.each do |statement, update_plans_all_time|
          file.puts Formatador.parse("[yellow]=========== #{statement.inspect} ============[/]")
          update_plans_all_time.each_with_index do |plans, ts|
            next if plans.empty?
            file.puts Formatador.parse("[blue]=========== for timestep: #{ts} ============[/]")
            weight = if weights.key?(statement)
                       weights[statement]
                     elsif weights.key?(statement.group)
                       weights[statement.group]
                     else
                       weights[statement.group][mix]
                     end
            next if weight.nil?

            total_cost = plans.sum_by(&:cost)

            file.puts "GROUP #{statement.group}" unless statement.group.nil?

            file.puts statement.label unless statement.label.nil?
            file.puts "#{statement.inspect} * #{weight.inspect} = " +
                        "#{weight.is_a?(Array) ? weight.map{|w| total_cost * w}.inject(:+)
                             : total_cost * weight}"
            plans.each do |plan|
              file.puts Formatador.parse(" for [magenta]#{plan.index.key}[/] " \
                                       "[yellow]$#{plan.cost}[/]")
              query_weights = Hash[plan.query_plans.map do |query_plan|
                [query_plan.query, weight]
              end]
              output_plans_one_timestep_txt plan.query_plans, file, 2, query_weights, ts

              plan.update_steps.each do |step|
                file.puts '  ' + step.inspect
              end

              file.puts
            end

            file.puts "\n"
          end
        end
      end

      def output_update_plan(plan, file, weight = nil, indents = 1)
        file.puts Formatador.parse(" for [magenta]#{plan.index.key}[/] " \
                                   "[yellow]$#{plan.cost}[/]")
        query_weights = Hash[plan.query_plans.map do |query_plan|
          [query_plan.query, weight]
        end]
        output_plans_txt plan.query_plans, file, 2, query_weights

        plan.update_steps.each do |step|
          file.puts '  ' * indents + step.inspect
        end

        file.puts
      end

      # Output update plans as text
      # @return [void]
      def output_update_plans_txt(update_plans, file, weights, mix = nil)
        unless update_plans.empty?
          header = "Update plans\n" + '━' * 50
          file.puts Formatador.parse("[blue]#{header}[/]")
        end

        update_plans.group_by(&:statement).each do |statement, plans|
          weight = if weights.key?(statement)
                     weights[statement]
                   elsif weights.key?(statement.group)
                     weights[statement.group]
                   else
                     weights[statement.group][mix]
                   end
          next if weight.nil?

          total_cost = plans.sum_by(&:cost)

          file.puts "GROUP #{statement.group}" unless statement.group.nil?

          file.puts statement.label unless statement.label.nil?
          file.puts "#{statement.inspect} * #{weight} = #{total_cost * weight}"
          plans.each do |plan|
            output_update_plan plan, file, weight
          end

          file.puts "\n"
        end
      end

      # @param Hash[statement, plans for each timestep]
      def time_depend_output_update_plans_diff_txt(td_update_plans, file)
        return if td_update_plans.nil?
        header = "Upseart plan diff \n" + '━' * 50
        file.puts Formatador.parse("[blue]#{header}[/]")
        td_update_plans.each do |stmt, plans|
          file.puts Formatador.parse("[yellow]============= #{stmt.text} ============[/]")
          plans.each_cons(2).to_a.each_with_index do |(former, current), i|
            file.puts Formatador.parse('  ' + "[blue]timestep: #{i} to #{i + 1}[/]")
            file.puts "  deleted plans ============================"
            former.reject{|f| current.any?{|c| compare_update_plans f, c}}.each do |deleted_plan|
              output_update_plan deleted_plan, file, -1, 2
            end
            file.puts "  added plans ============================"
            current.reject{|c| former.any?{|f| compare_update_plans f, c}}.each do |added_plan|
              output_update_plan added_plan, file, -1, 2
            end
          end
        end
        file.puts "\n"
      end

      def compare_update_plans(left , right)
        return false if left.index != right.index
        return false if left.query_plans != right.query_plans
        return false if left.statement != right.statement
        return false if left.update_fields != right.update_fields
        return false if left.update_steps != right.update_steps
        true
      end

      # Output the results of advising as text
      # @return [void]
      def output_txt(result, file = $stdout, enumerated = false,
                     _backend = nil)
        if enumerated
          header = "Enumerated indexes\n" + '━' * 50
          output_indexes_txt header, result.enumerated_indexes, file
        end

        header = "Indexes\n" + '━' * 50
        if result.is_a? NoSE::Search::TimeDependResults
          #time_depend_output_indexes_txt header, result.indexes, file
          indexes_each_ts = result.time_depend_indexes.indexes_all_timestep.map{|tdi| tdi.indexes}
          time_depend_output_indexes_txt header, indexes_each_ts, file
        else
          output_indexes_txt header, result.indexes, file
        end

        file.puts Formatador.parse('  Total size: ' \
                                   "[blue]#{result.total_size}[/]\n\n")

        # Output query plans for the discovered indices
        header = "Query plans\n" + '━' * 50
        file.puts Formatador.parse("[blue]#{header}[/]")
        weights = result.workload.statement_weights
        weights = result.weights if weights.nil? || weights.empty?

        if result.is_a? NoSE::Search::TimeDependResults
          time_depend_output_plans_txt result.plans, file, 1, weights
        else
          output_plans_txt result.plans, file, 1, weights
        end

        result.update_plans = [] if result.update_plans.nil?
        if result.is_a? NoSE::Search::TimeDependResults
          time_depend_output_update_plans_txt result.update_plans, file, weights,
                                              result.workload.mix

          time_depend_output_update_plans_diff_txt reserialized_update_plans, file
        else
          output_update_plans_txt result.update_plans, file, weights,
                                  result.workload.mix
        end

        if result.is_a? NoSE::Search::TimeDependResults
          output_migration_plans_txt result.migrate_plans, file, 1
        end

        file.puts Formatador.parse('  Total cost: ' \
                                   "[blue]#{result.is_a?(NoSE::Search::TimeDependResults) \
                                    ? result.each_total_cost : result.total_cost}[/]\n")
      end

      # Output an HTML file with a description of the search results
      # @return [void]
      def output_html(result, file = $stdout, enumerated = false,
                      backend = nil)
        # Get an SVG diagram of the model
        tmpfile = Tempfile.new %w(model svg)
        result.workload.model.output :svg, tmpfile.path, true
        svg = File.open(tmpfile.path).read

        enumerated &&= result.enumerated_indexes
        tmpl = File.read File.join(File.dirname(__FILE__),
                                   '../templates/report.erb')
        ns = OpenStruct.new svg: svg,
                            backend: backend,
                            indexes: result.indexes,
                            enumerated_indexes: enumerated,
                            workload: result.workload,
                            update_plans: result.update_plans,
                            plans: result.plans,
                            total_size: result.total_size,
                            total_cost: result.total_cost

        force_colour
        file.write ERB.new(tmpl, nil, '>').result(ns.instance_eval { binding })
      end

      # Output the results of advising as JSON
      # @return [void]
      def output_json(result, file = $stdout, enumerated = false,
                      _backend = nil)
        # Temporarily remove the enumerated indexes
        if enumerated
          enumerated = result.enumerated_indexes
          result.delete_field :enumerated_indexes
        end

        if result.is_a? NoSE::Search::TimeDependResults
          file.puts JSON.pretty_generate \
          Serialize::SearchTimeDependResultRepresenter.represent(result).to_hash
        else
          file.puts JSON.pretty_generate \
          Serialize::SearchResultRepresenter.represent(result).to_hash
        end

        result.enumerated_indexes = enumerated if enumerated
      end

      # Output the results of advising as YAML
      # @return [void]
      def output_yml(result, file = $stdout, enumerated = false,
                     _backend = nil)
        # Temporarily remove the enumerated indexes
        if enumerated
          enumerated = result.enumerated_indexes
          result.delete_field :enumerated_indexes
        end

        file.puts Serialize::SearchResultRepresenter.represent(result).to_yaml

        result.enumerated_indexes = enumerated if enumerated
      end

      # Filter an options hash for those only relevant to a given command
      # @return [Thor::CoreExt::HashWithIndifferentAccess]
      def filter_command_options(opts, command)
        Thor::CoreExt::HashWithIndifferentAccess.new(opts.select do |key|
          self.class.commands[command].options \
            .each_key.map(&:to_sym).include? key.to_sym
        end)
      end

      # Enable forcing the colour or no colour for output
      # We just lie to Formatador about whether or not $stdout is a tty
      # @return [void]
      def force_colour(colour = true)
        stdout_metaclass = class << $stdout; self; end
        method = colour ? ->() { true } : ->() { false }
        stdout_metaclass.send(:define_method, :tty?, &method)
      end
    end
  end
end

require_relative 'nose_cli/shared_options'

# Require the various subcommands
require_relative 'nose_cli/analyze'
require_relative 'nose_cli/benchmark'
require_relative 'nose_cli/td_benchmark'
require_relative 'nose_cli/collect_results'
require_relative 'nose_cli/create'
require_relative 'nose_cli/diff_plans'
require_relative 'nose_cli/dump'
require_relative 'nose_cli/export'
require_relative 'nose_cli/execute'
require_relative 'nose_cli/list'
require_relative 'nose_cli/load'
require_relative 'nose_cli/genworkload'
require_relative 'nose_cli/graph'
require_relative 'nose_cli/plan_schema'
require_relative 'nose_cli/proxy'
require_relative 'nose_cli/random_plans'
require_relative 'nose_cli/reformat'
require_relative 'nose_cli/repl'
require_relative 'nose_cli/recost'
require_relative 'nose_cli/search'
require_relative 'nose_cli/search_migrations'
require_relative 'nose_cli/view_result_txt'
require_relative 'nose_cli/search_all'
require_relative 'nose_cli/search_bench'
require_relative 'nose_cli/search_pattern'
require_relative 'nose_cli/texify'
require_relative 'nose_cli/why'

# Only include the console command if pry is available
begin
  require 'pry'
  require_relative 'nose_cli/console'
rescue LoadError
  nil
end
