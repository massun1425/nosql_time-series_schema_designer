# frozen_string_literal: true

require 'table_print'
require 'gruff'

module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor
      desc 'execute PLANS', 'test performance of the named PLANS'

      long_desc <<-LONGDESC
        `nose execute` is similar to `nose benchmark`. It will take manually
        defined plans with the given name stored in the `plans` subdirectory,
        execute each statement, and output a summary of the execution times.
        Before runnng execute, `nose create` and `nose load` must be used to
        prepare the target database.
      LONGDESC

      shared_option :mix

      option :num_iterations, type: :numeric, default: 100,
             banner: 'the number of times to execute each ' \
                                      'statement'
      option :repeat, type: :numeric, default: 1,
             banner: 'how many times to repeat the benchmark'
      option :group, type: :string, default: nil, aliases: '-g',
             banner: 'restrict the benchmark to statements in the ' \
                             'given group'
      option :fail_on_empty, type: :boolean, default: true,
             banner: 'abort if a column family is empty'
      option :totals, type: :boolean, default: false, aliases: '-t',
             banner: 'whether to include group totals in the output'
      option :format, type: :string, default: 'txt',
             enum: %w(txt csv), aliases: '-f',
             banner: 'the format of the output data'

      def execute(plans_name)
        # Load the execution plans
        plans = Plans::ExecutionPlans.load plans_name

        # Construct an instance of the backend
        result = OpenStruct.new
        result.workload = Workload.new plans.schema.model
        result.workload.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && result.workload.mix != :default
        result.model = result.workload.model
        result.indexes = plans.schema.indexes.values
        backend = get_backend(options, result)

        # Get sample index values to use in queries
        index_values = index_values plans.schema.indexes.values, backend,
                                    options[:num_iterations],
                                    options[:fail_on_empty]

        table = []
        total = 0
        plans.groups.each do |group, group_plans|
          next if options[:group] && group != options[:group]

          group_table = []
          group_total = 0
          group_weight = plans.weights[group][result.workload.mix]
          next unless group_weight

          group_plans.each do |plan|
            next if options[:plan] && plan.name != options[:plan]

            update = !plan.steps.last.is_a?(Plans::IndexLookupPlanStep)
            method = update ? :bench_update : :bench_query
            measurement = send method, backend, plans.schema.indexes.values,
                               plan, index_values,
                               options[:num_iterations],
                               options[:repeat], weight: group_weight

            # Run the query and get the total time
            group_total += measurement.mean
            group_table << measurement
          end

          if options[:totals]
            total_measurement = Measurements::Measurement.new nil, 'TOTAL'
            total_measurement << group_table.map(&:weighted_mean) \
                                 .inject(0, &:+)
            group_table << total_measurement
          end

          table << OpenStruct.new(label: plans_name, group: group,
                                  measurements: group_table)
          group_total *= group_weight
          total += group_total
        end

        if options[:totals]
          total_measurement = Measurements::Measurement.new nil, 'TOTAL'
          total_measurement << table.map do |group|
            group.measurements.find { |m| m.name == 'TOTAL' }.mean
          end.inject(0, &:+)
          table << OpenStruct.new(label: plans_name, group: 'TOTAL',
                                  measurements: [total_measurement])
        end

        case options[:format]
        when 'txt'
          output_table table
        else
          output_csv table
        end
      end

      private

      # Output the table of results
      # @return [void]
      def output_table(table)
        columns = [
            'label', 'group',
            { 'measurements.name' => { display_name: 'name' } },
            { 'measurements.weight' => { display_name: 'weight' } },
            { 'measurements.mean' => { display_name: 'mean' } },
            { 'measurements.estimate' => { display_name: 'cost' } }
        ]

        tp table, *columns
      end

      # Output a CSV file of results
      # @return [void]
      def output_csv(table)
        csv_str = CSV.generate do |csv|
          csv << %w(label group name weight mean cost)

          table.each do |group|
            group.measurements.each do |measurement|
              csv << [
                  group.label,
                  group.group,
                  measurement.name,
                  measurement.weight,
                  measurement.mean,
                  measurement.estimate
              ]
            end
          end
        end

        puts csv_str
      end

      # Get the average execution time for a single query plan
      # @return [Measurements::Measurement]
      def bench_query(backend, indexes, plan, index_values, iterations, weight: 1.0)

        condition_list = execute_conditions plan.params, indexes, index_values,
                                            iterations
        prepared = backend.prepare_query nil, plan.select_fields, plan.params,
                                         [plan.steps]

        measurement = Measurements::Measurement.new plan, weight: weight

        retry_times = 10
        current_times = 0
        begin
          # Execute each plan and measure the time
          condition_list.each do |conditions|
            start_time = Time.now.utc
            prepared.execute conditions
            measurement << (Time.now.utc - start_time)
          end
          # plot_each_measurement each_execution_measurements
          GC.start
        rescue ThreadError
          current_times += 1
          STDERR.puts "ThreadError raised. Sleep before next retry"
          sleep(20)
          retry if current_times < retry_times
          raise
        end
        #puts executed_results.inspect

        measurement
      end

      def exec_query(backend, plan, index_values)
        indexes = plan.select do |step|
          step.is_a? Plans::IndexLookupPlanStep
        end.map(&:index)

        condition_list = execute_all_conditions plan.params, indexes, index_values
        prepared = backend.prepare_query plan.query, plan.select_fields, plan.params,
                                         [plan.steps]

        condition_list.map { |conditions| prepared.execute conditions }.flatten(1)
      end

      # Get the average execution time for a single update plan
      # @return [Measurements::Measurement]
      def bench_update(backend, indexes, plan, index_values,
                       iterations, weight: 1.0, nullable_indexes: nil)
        condition_list = execute_conditions plan.params, indexes, index_values,
                                            iterations

        # Get values for the fields which were provided as parameters
        fields = plan.update_steps.last.fields.select do |field|
          plan.params.key? field.id
        end
        setting_list = 1.upto(iterations).map do |i|
          fields.map do |field|
            # First check for IDs given as part of the query otherwise
            # get the backend to generate a random ID or take a random value
            condition = condition_list[i - 1][field.id]

            value = if !condition.nil?
                      condition.value
                    elsif field.is_a?(Fields::IDField)
                      backend.generate_id
                    else
                      field.random_value
                    end

            FieldSetting.new(field, value)
          end
        end

        prepared = backend.prepare_update nil, [plan]

        measurement = Measurements::Measurement.new plan, weight: weight

        # Execute each plan and measure the time
        setting_list.zip(condition_list).each do |settings, conditions|
          start_time = Time.now.utc
          prepared.each { |p| p.execute settings, conditions }
          measurement << Time.now.utc - start_time
        end
        GC.start

        measurement
      end

      # Construct a list of values to be substituted in the plan
      # @return [Array<Hash>]
      def execute_conditions(params, indexes, index_values, iterations)
        1.upto(iterations).map do |i|
          Hash[params.map do |field_id, condition|
            value = nil
            indexes.each do |index|
              values = index_values[index]
              next if values.empty?
              begin
                value = values[i % values.length][condition.field.id]
              rescue Exception => e
                puts e
                throw e
              end
              break unless value.nil?
            end

            [
                field_id,
                Condition.new(condition.field, condition.operator, value)
            ]
          end]
        end
      end

      def execute_all_conditions(params, indexes, index_values)
        indexes.map do |index|
          index_values[index].map do |row|
            Hash[params.map do |field_id, condition|
              next if row.empty?
              value = row[condition.field.id]
              next if value.nil?
              [
                  field_id,
                  Condition.new(condition.field, condition.operator, value)
              ]
            end]
          end
        end.flatten
      end

      def plot_each_measurement(measurements)
        FileUtils.mkdir_p "./measure/"
        g = Gruff::Line.new
        g.title = plan.query.comment.strip
        g.dataxy 'measurement', (0...measurements.size).to_a, measurements
        g.minimum_value = 0
        g.write("./measure/#{plan.query.comment.gsub("--", "").strip}.png")
        puts measurements.inspect
      end
    end
  end
end
