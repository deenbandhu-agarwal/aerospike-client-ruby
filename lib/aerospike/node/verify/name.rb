# frozen_string_literal: true

module Aerospike
  class Node
    module Verify
      module Name
        class << self
          def call(node, info_map)
            info_name = info_map['node']

            if !info_name
              node.decrease_health
              raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_NODE_ERROR, "Node name is empty")
            end

            if !(node.name == info_name)
              # Set node to inactive immediately.
              node.inactive!
              raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_NODE_ERROR, "Node name has changed. Old=#{node.name} New= #{info_name}")
            end
          end
        end
      end
    end
  end
end