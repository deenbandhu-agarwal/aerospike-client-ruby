# frozen_string_literal: true

module Aerospike
  class Node
    module Refresh
      module Info
        class << self
          def call(node, peers)
            conn = node.get_connection(1)
            if peers.use_peers?
              info_map = ::Aerospike::Info.request(conn, *INFO_CMDS_PEERS)
              Verify::PeersGeneration.(node, info_map, peers)
              Verify::PartitionGeneration.(node, info_map, peers)
            else
              info_map = ::Aerospike::Info.request(conn, *INFO_CMDS_SERVICES)
              Verify::PartitionGeneration.(node, info_map, peers)
              add_friends(info_map, peers)
            end

            Verify::Name.(node, info_map)
            Verify::ClusterName.(node, info_map)

            node.restore_health
            node.responded!

            peers.refresh_count += 1
            node.failures.value = 0
          rescue => e
            conn.close if conn
            node.decrease_health
            peers.generation_changed = true if peers.use_peers?
            Refresh::Failed.(node, e)
          end
        end
      end
    end
  end
end
