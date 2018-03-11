# frozen_string_literal: true

module Aerospike
  class Node
    module Verify
      module ClusterName
        class << self
          def call(node, info_map)
            if node.cluster_name && node.cluster_name != info_map['cluster-name']
              node.inactive!
              raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_NODE_ERROR, "Cluster name does not match. expected: #{cluster_name}, got: #{info_map['cluster-name']}")
            end
          end
        end
      end
    end
  end
end
