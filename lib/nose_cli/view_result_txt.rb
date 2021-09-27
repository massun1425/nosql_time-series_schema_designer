# frozen_string_literal: true

require 'csv'
require 'table_print'

module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor
      desc 'benchmark PLAN_FILE', 'test performance of plans in PLAN_FILE'

      long_desc <<-LONGDESC
        ` convert json output file to txt output file
      LONGDESC

      def view_result_txt(plan_file)
        file = if options[:output].nil?
                 $stdout
               else
                 File.open(options[:output], 'w')
               end

        result, backend = load_time_depend_plans plan_file, options
        send(('output_txt').to_sym, result, file, options[:enumerated], backend)
      end
    end
  end
end
