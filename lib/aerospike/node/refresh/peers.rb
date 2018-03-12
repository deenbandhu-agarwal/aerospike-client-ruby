# frozen_string_literal: true

module Aerospike
  class Node
    module Refresh
      module Peers
        class << self
          def call(node, peers)
            return if node.failures.value > 0 || !node.active?

            collection = ::Aerospike::Peers::Fetch.(node.cluster, node.tend_connection)
            peers.peers = collection.peers
            peers_validated = true

            peers.peers.each do |peer|
              next if ::Aerospike::Cluster::FindNode.(node.cluster, peers, peer.node_name)

              node_validated = false

              peer.hosts.each do |host|
                begin
                  nv = NodeValidator.new(node.cluster, node.host, node.cluster.connection_timeout, node.cluster.ssl_options)

                  if nv.name != peer.node_name
                    # TODO:
                    # Must look for new node name in the unlikely event that node names do not agree.
                    break;
                  end

                  node = node.cluster.create_node(nv)
                  peers.nodes[nv.name] = node
                  node_validated = true
                  break;
                rescue ::Aerospike::Exceptions::Aerospike => e
                  Aerospike.logger.warn("Add node #{host} failed: #{e.inspect}")
                end

                peers_validated = false
              end
            end

            node.peers_generation.value = collection.generation if peers_validated
            peers.refresh_count += 1
          rescue ::Aerospike::Exceptions::Aerospike => e
            Refresh::Failed.(node, e)
          end
        end
      end
    end
  end
end
