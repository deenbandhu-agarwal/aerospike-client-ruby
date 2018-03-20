# frozen_string_literal: true

module Aerospike
  class Node
    module Refresh
      # Reset a node before running a refresh cycle
      module Reset
        class << self
          def call(node)
            node.reset_reference_count!
            node.reset_responded!
            node.partition_generation.reset_changed!
          end
        end
      end
    end
  end
end
