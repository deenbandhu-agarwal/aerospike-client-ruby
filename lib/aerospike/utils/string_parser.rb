# frozen_string_literal: true

module Aerospike
  module Utils
    class StringParser
      attr_reader :io
      def initialize(str)
        @io = ::StringIO.new(str)
      end

      def current
        @io.string[@io.tell]
      end

      # Reads next character and raise if not matching desired one
      def expect(char)
        raise ::Aerospike::Exceptions::Parse unless @io.read(1) == char
      end

      def read_until(char)
        [].tap do |result|
          loop do
            chr = @io.read(1)
            break if chr == char
            result << chr
          end
        end.join
      end

      def step(count = 1)
        @io.read(count)
      end

    end
  end
end
