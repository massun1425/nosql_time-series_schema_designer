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
      desc 'search_storage_const NAME', 'run the workload NAME'

      long_desc <<-LONGDESC
        `nose search_storage const` is the support command for search command. It will search with reduced storage constraint
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
      option :amplify, type: :boolean, default: false,
             desc: 'whether amplify workload for larger workload'
      option :frequency_type, type: :string,
             enum: %w(time_depend static firstTs lastTs),
             desc: 'choose frequency type of workload'

      def search_storage_const(name)
        result = search(name, Float::INFINITY)
        no_str_const_size = result.total_size.max

        storage_const_ratios = [0.8, 0.85, 0.9, 0.95]

        storage_const_ratios.each do |r|
          puts "++++++++++++++++++++++++++++++++++++++++++++++++++"
          puts "++++++++++++++++++++++++++++++++++++++++++++++++++"
          puts "storage reduction ration #{r}, (full size #{no_str_const_size}) * (reduction ratio #{r}) = #{no_str_const_size * r}"
          puts "++++++++++++++++++++++++++++++++++++++++++++++++++"
          puts "++++++++++++++++++++++++++++++++++++++++++++++++++"
          RunningTimeLogger.clear
          GC.start
          begin
            search(name, no_str_const_size * r)
          rescue NoSE::Exception::NoSolutionException => e
            puts e.inspect
          end
        end
        result
      end
    end
  end
end
