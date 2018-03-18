# frozen_string_literal: true

module Aerospike
  class Node
    # generic class for representing changes in eg. peer and partition generation
    class Generation
      attr_reader :number

      def initialize(number = -1)
        @number = ::Aerospike::Atomic.new(number)
        @changed = ::Aerospike::Atomic.new(false)
      end

      def changed?
        @changed.value == true
      end

      def eql?(number)
        @number.value == number
      end

      def reset_changed!
        @changed.value = false
      end

      def update(new_number)
        return if @number.value == new_number
        @number.value = new_number
        @changed.value = true
      end
    end
  end
end
