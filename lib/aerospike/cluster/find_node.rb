# frozen_string_literal: true

module Aerospike
  class Cluster
    # Find node in cluster by name
    module FindNode
      class << self
        def call(cluster, peers, node_name)
          node = cluster.find_node_by_name(node_name) || peers.find_node_by_name(node_name)
          return if node.nil?
          node.tap do |n|
            n.increase_reference_count!
          end
        end
      end
    end
  end
end
