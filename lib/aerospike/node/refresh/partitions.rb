# frozen_string_literal: true

module Aerospike
  class Node
    module Refresh
      module Partitions
        class << self
          def call(node, e)
            conn = node.get_connection(1)
            node.cluster.update_partitions(conn, node)
          rescue ::Aerospike::Exceptions::Aerospike => e
            conn.close if conn
            Refresh::Failed.(node, e)
          end
        end
      end
    end
  end
end
