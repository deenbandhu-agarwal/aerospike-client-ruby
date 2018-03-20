# frozen_string_literal: true

module Aerospike
  class Node
    module Refresh
      # Refresh peers/friends based on old service protocol
      module Friends
        class << self
          def call(node, peers, info_map)
            friend_string = info_map['services']
            cluster = node.cluster

            if friend_string.to_s.empty?
              node.peers_count.value = 0
              return
            end

            friend_names = friend_string.split(';')
            node.peers_count.value = friend_names.size

            friend_names.each do |friend|
              hostname, port = friend.split(':')
              host = Host.new(hostname, port.to_i)
              found_node = cluster.find_alias(host)

              if found_node
                found_node.increase_reference_count!
              else
                unless peers.hosts.include?(host)
                  prepare(cluster, peers, host)
                end
              end
            end
          end

          def prepare(cluster, peers, host)
            nv = NodeValidator.new(
              cluster,
              host,
              cluster.connection_timeout,
              cluster.cluster_name,
              cluster.ssl_options
            )

            node = peers.find_node_by_name(nv.name)

            unless node.nil?
              peers.hosts << host
              node.aliases << host
              return true
            end

            node = cluster.find_node_by_name(nv.name)

            unless node.nil?
              peers.hosts << host
              node.aliases << host
              # Only increase reference count if found in cluster
              node.increase_reference_count!
              cluster.add_alias(host, node)
              return true
            end

            node = cluster.create_node(nv)
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
