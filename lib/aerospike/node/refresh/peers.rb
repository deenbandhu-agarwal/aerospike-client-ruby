# frozen_string_literal: true

module Aerospike
  class Node
    module Refresh
      module Peers
        class << self
          def call(node, peers)
            return unless should_refresh?(node)

            ::Aerospike.logger.debug("Update peers for node #{node.name}")

            collection = ::Aerospike::Peers::Fetch.(node.cluster, node.tend_connection)
            peers.peers = collection.peers
            node.peers_count.value = peers.peers.size
            peers_validated = true

            peers.peers.each do |peer|
              next if ::Aerospike::Cluster::FindNode.(node.cluster, peers, peer.node_name)

              node_validated = false

              peer.hosts.each do |host|
                begin
                  nv = NodeValidator.new(node.cluster, host, node.cluster.connection_timeout, node.cluster.cluster_name, node.cluster.ssl_options)

                  if nv.name != peer.node_name
                    ::Aerospike.logger.warn("Peer node #{peer.node_name} is different than actual node #{nv.name} for host #{host}");
                    # Must look for new node name in the unlikely event that node names do not agree.
                    # Node already exists. Do not even try to connect to hosts.
                    if Cluster::FindNode.(node.cluster, peers, nv.name)
                      node_validated = true
                      break;
                    end
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

            # Only set new peers generation if all referenced peers are added to
            # the cluster.
            node.peers_generation.update(collection.generation) if peers_validated
            peers.refresh_count += 1
          rescue ::Aerospike::Exceptions::Aerospike => e
            Refresh::Failed.(node, e)
          end

          def should_refresh?(node)
            node.failures.value == 0 && node.active?
          end
        end
      end
    end
  end
end
