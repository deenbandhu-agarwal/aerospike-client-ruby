# frozen_string_literal: true

module Aerospike
  class Cluster
    module FindNode
      class << self
        def call(cluster, peers, node_name)
          node = cluster.find_node_by_name(node_name) || peers.nodes[node_name]
          return false if node.nil?
          node.tap do |n|
            n.reference_count.update { |v| v + 1 }
          end
        end
      end
    end
  end
end
