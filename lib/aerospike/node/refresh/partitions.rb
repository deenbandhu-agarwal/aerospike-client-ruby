# frozen_string_literal: true

module Aerospike
  class Node
    module Refresh
      module Partitions
        class << self
          def call(node, peers)
            return unless should_refresh?(node, peers)

            conn = node.tend_connection
            node.cluster.update_partitions(conn, node)
          rescue ::Aerospike::Exceptions::Aerospike => e
            conn.close if conn
            Refresh::Failed.(node, e)
          end

          # Do not refresh partitions when node connection has already failed
          # during this cluster tend iteration. Also, avoid "split cluster"
          # case where this node thinks it's a 1-node cluster. Unchecked, such
          # a node can dominate the partition map and cause all other nodes to
          # be dropped.
          def should_refresh?(node, peers)
            return false if node.failed? || !node.active?
            return false if !node.has_peers? && peers.refresh_count > 1
            true
          end
        end
      end
    end
  end
end
