# frozen_string_literal: true

module Aerospike
  class Node
    module Refresh
      module Info
        CMDS_BASE = %w[node partition-generation cluster-name].freeze
        CMDS_PEERS = (CMDS_BASE + ['peers-generation']).freeze
        CMDS_SERVICES = (CMDS_BASE + ['services']).freeze

        class << self
          def call(node, peers)
            conn = node.get_connection(1)
            if peers.use_peers?
              info_map = ::Aerospike::Info.request(conn, *CMDS_PEERS)
              Verify::PeersGeneration.(node, info_map, peers)
              Verify::PartitionGeneration.(node, info_map, peers)
            else
              info_map = ::Aerospike::Info.request(conn, *CMDS_SERVICES)
              Verify::PartitionGeneration.(node, info_map, peers)
              Refresh::Friends.(node, peers, info_map)
            end

            Verify::Name.(node, info_map)
            Verify::ClusterName.(node, info_map)

            node.restore_health
            node.responded!

            peers.refresh_count += 1
            node.reset_failures!
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
