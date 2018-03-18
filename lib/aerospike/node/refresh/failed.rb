# frozen_string_literal: true

module Aerospike
  class Node
    module Refresh
      module Failed
        class << self
          def call(node, e)
            Aerospike.logger.info("Node #{node.name} refresh failed #{e.inspect}")
            node.failures.update { |v| v + 1 }
          end
        end
      end
    end
  end
end
