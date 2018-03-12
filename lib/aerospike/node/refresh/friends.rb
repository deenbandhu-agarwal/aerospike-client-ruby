# frozen_string_literal: true

module Aerospike
  class Node
    module Refresh
      # Refresh peers/friends based on old service protocol
      module Friends
        class << self
          def call(node, peers, info_map)
            friend_string = info_map['services']

            if friend_string.to_s.empty?
              node.peers_count.value = 0
              return
            end

            friend_names = friend_string.split(';')
            node.peers_count.value = friend_names.size

            friend_names.each do |friend|
              hostname, port = friend.split(':')
              host = Host.new(hostname, port.to_i)
              node = node.cluster.find_alias(host)

              if node
                node.increase_reference_count!
              else
                unless peers.hosts.any? {|h| h == host}
                  prepare(node, peers, host)
                end
              end
            end
          end

          def prepare(node, peers, host)
            nv = NodeValidator.new(
              node.cluster,
              host,
              node.timeout,
              node.cluster.connection_timeout,
              node.cluster.cluster_name,
              node.cluster.ssl_options
            )

            node = peers.nodes[nv.name]

            unless node.nil?
              peers.hosts << host
              node.aliases << host
              return true
            end

            node = node.cluster.nodes_map[nv.name]

            unless node.nil?
              peers.hosts << host
              node.aliases << host
              node.increase_reference_count!
              cluster.aliases[host.to_s] = node
              return true
            end

            node = ::Aerospike::Cluster::FindNode.(node.cluster, peers, nv.name)
            unless node.nil?
              peers.hosts << host
              node.aliases << host
              node.cluster.add_alias(host, node)
              return true
            end

            node = node.cluster.create_node(nv)
            peers.hosts << host
            peers.nodes[nv.name] = node
            true
          rescue ::Aerospike::Exceptions::Aerospike
            false
          end
        end
      end
    end
  end
end
