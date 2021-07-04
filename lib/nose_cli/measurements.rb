# frozen_string_literal: true

require 'descriptive_statistics/safe'
require 'forwardable'

module NoSE
  # Storage and presentation of value from performance measusrements
  module Measurements
    # A measurement of a single statement execution time
    class Measurement
      attr_accessor :estimate
      attr_reader :plan, :name, :weight, :values

      # Allow the values array to store numbers and compute stats
      extend Forwardable
      def_delegators :@values, :each, :<<, :size, :count, :length, :empty?, :standard_error, :middle_mean

      include Enumerable
      include DescriptiveStatistics

      def initialize(plan, name = nil, estimate = nil, weight: 1.0)
        @plan = plan
        @name = name || (plan && plan.name)
        @estimate = estimate
        @weight = weight
        @values = []
      end

      # The mean weighted by this measurement weight
      # @return [Fixnum]
      def weighted_mean(timestep = nil)
        (timestep.nil? ? @weight : @weight[timestep]) * mean
      end

      def middle_mean
        sorted_values = @values.sort
        middle_values = sorted_values[1..-2]
        middle_values.reduce(:+).to_f / middle_values.size
      end

      def standard_error
        sum = @values.reduce(:+)
        mean = sum.to_f / @values.size
        var = @values.reduce(0){|a, b| a + (b - mean) ** 2} / (@values.size - 1)
        standard_deviation = var ** 0.5
        standard_error = standard_deviation / (size ** 0.5)
        standard_error
      end
    end
  end
end
