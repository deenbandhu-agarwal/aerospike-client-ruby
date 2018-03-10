# frozen_string_literal: true

# Copyright 2014-2017 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'aerospike/atomic/atomic'

module Aerospike
  class Node

    attr_reader :reference_count, :responded, :name, :features, :cluster_name, :partition_changed, :peers_generation

    PARTITIONS = 4096
    FULL_HEALTH = 100

    # Initialize server node with connection parameters.
    def initialize(cluster, nv)
      @cluster = cluster
      @name = nv.name
      @aliases = Atomic.new(nv.aliases)
      @host = nv.host
      @use_new_info = Atomic.new(nv.use_new_info)
      @features = nv.features
      @cluster_name = nv.cluster_name

      # Assign host to first IP alias because the server identifies nodes
      # by IP address (not hostname).
      @host = nv.aliases[0]
      @health = Atomic.new(FULL_HEALTH)
      @peers_count = Atomic.new(0)
      @peers_generation = Atomic.new(-1)
      @partition_generation = Atomic.new(-1)
      @reference_count = Atomic.new(0)
      @responded = Atomic.new(false)
      @active = Atomic.new(true)
      @partition_changed = Atomic.new(false)
      @failures = 0

      @connections = Pool.new(@cluster.connection_queue_size)

      # TODO: put in separate methods
      @connections.create_block = Proc.new do
        while conn = Connection::Create.(
            @host.name, @host.port,
            tls_name: @host.tls_name,
            timeout: @cluster.connection_timeout,
            ssl_options: @cluster.ssl_options
          )

          # need to authenticate
          if @cluster.user && @cluster.user != ''
            begin
              command = AdminCommand.new
              command.authenticate(conn, @cluster.user, @cluster.password)
            rescue => e
              # Socket not authenticated. Do not put back into pool.
              conn.close if conn
              raise e
            end
          end

          break if conn.connected?
        end
        conn
      end

      @connections.cleanup_block = Proc.new { |conn| conn.close if conn }
    end

    def refresh(peers)
      conn = get_connection(1)
      if peers.use_peers?
        info_map = Info.request(conn, 'node', 'peers-generation', 'partition-generation')
        verify_node_name(info_map)
        verify_peers_generation(info_map, peers)
        verify_partition_generation(info_map)
      else
        info_map = Info.request(conn, 'node', 'partition-generation', 'services', 'cluster-name')
        verify_node_name(info_map)
        verify_partition_generation(info_map)
        add_friends(info_map, peers)
      end

      verify_node_name_and_cluster_name(info_map)
      restore_health
      @responded.update { |_| true }

      peers.refresh_count += 1
      failures = 0
    rescue => e
#      puts e.inspect
      conn.close if conn
      decrease_health
      peers.generation_changed == true if peers.use_peers?
      refresh_failed(e)
    end

    def verify_node_name(info_map)
      info_name = info_map.fetch('node') do
        fail ::Aerospike::Exceptions::Parse
      end
      if @name != info_name
        @active.update {|_| false }
        fail ::Aerospike::Exceptions::Aerospike
      end
    end

    # Fetch and set peers generation. If peers needs to be refreshed this
    # will be indicated in @peers_changed
    def verify_peers_generation(info_map, peers)
      gen_string = info_map.fetch('peers-generation')

      raise Aerospike::Exceptions::Parse.new('peers-generation is empty') if gen_string.to_s.empty?

      generation = gen_string.to_i

      if @peers_generation.value != generation
        Aerospike.logger.info("Node #{get_name} peers generation #{generation} changed")
        peers.generation_changed = true
        @peers_generation.value = generation
      end
    end

    # Fetch and set partition generation. If partitions needs to be refreshed this
    # will be indicated in @partition_changed
    def verify_partition_generation(info_map)
      gen_string = info_map.fetch('partition-generation')

      raise Aerospike::Exceptions::Parse.new('partition-generation is empty') if gen_string.to_s.empty?

      generation = gen_string.to_i

      if @partition_generation.value != generation
        Aerospike.logger.info("Node #{get_name} partition generation #{generation} changed")
        @partition_changed.value = true
        @partition_generation.value = generation
      end
    end

    def add_friends(info_map, peers)
    end

    def prepare_friend(host, peers)
      # TODO validate
      nv = NodeValidator.new(
        @cluster, host, timeout,
        @cluster.connection_timeout, @cluster.cluster_name, @cluster.ssl_options
      )

      node = peers.nodes[nv.name]

      unless node.nil?
        peers.hosts << host
        node.aliases << host
        return true
      end

      node = @cluster.nodes_map[nv.name]

      unless node.nil?
        peers.hosts << host
        node.aliases << host
        node.reference_count.update { |v| v + 1 }
        cluster.aliases[host.to_s] = node
        return true
      end

      node = @cluster.create_node(nv)
      peers.hosts << host
      peers.nodes[nv.name] = node
      true
    rescue => e
      false
    end

    def refresh_peers(peers)
      return if @failures > 0 || !active?

      collection = ::Aerospike::Peers::Fetch.(@cluster, get_connection(1))
      peers.peers = collection.peers
      peers_validated = true

      peers.peers.each do |peer|
        next if ::Aerospike::Cluster::FindNode.(@cluster, peers, peer.node_name)

        node_validated = false

        peer.hosts.each do |host|
          begin
            nv = NodeValidator.new(@cluster, @host, @cluster.connection_timeout, @cluster.ssl_options)

            if nv.name != peer.node_name
              # TODO:
              # Must look for new node name in the unlikely event that node names do not agree.
              break;
            end

            node = @cluster.create_node(nv)
            peers.nodes[nv.name] = node
            node_validated = true
            break;
          rescue => e
          end

          peers_validated = false
        end

        @peers_generation.value = collection.generation if peers_validated
        peers.refresh_count += 1
      end
    rescue => e
      refresh_failed(e)
    end

    def refresh_partitions(peers)
      conn = get_connection(1)
      @cluster.update_partitions(conn, self)
    rescue => e
      conn.close if conn
      refresh_failed(e)
    end

    def refresh_failed(e)
      Aerospike.logger.info("Node #{get_name} refresh failed #{e.inspect}")
      @failures += 1
    end

    def partition_changed?
      @partition_changed.value == true
    end

    # Get a connection to the node. If no cached connection is not available,
    # a new connection will be created
    def get_connection(timeout)
      while true
        conn = @connections.poll
        if conn.connected?
          conn.timeout = timeout.to_f
          return conn
        end
      end
    end

    # Put back a connection to the cache. If cache is full, the connection will be
    # closed and discarded
    def put_connection(conn)
      conn.close if !@active.value
      @connections.offer(conn)
    end

    # Mark the node as healthy
    def restore_health
      # There can be cases where health is full, but active is false.
      # Once a node has been marked inactive, it stays inactive.
      @health.value = FULL_HEALTH
    end

    # Decrease node Health as a result of bad connection or communication
    def decrease_health
      @health.update {|v| v -= 1 }
    end

    # Check if the node is unhealthy
    def unhealthy?
      @health.value <= 0
    end

    # Retrieves host for the node
    def get_host
      @host
    end

    # Checks if the node is active
    def active?
      @active.value
    end

    # Returns node name
    def get_name
      @name
    end

    # Returns node aliases
    def get_aliases
      @aliases.value
    end

    # Adds an alias for the node
    def add_alias(alias_to_add)
      # Aliases are only referenced in the cluster tend threads,
      # so synchronization is not necessary.
      aliases = get_aliases
      aliases ||= []

      aliases << alias_to_add
      set_aliases(aliases)
    end

    # Marks node as inactice and closes all cached connections
    def close
      @active.value = false
      close_connections
    end

    def supports_feature?(feature)
      @features.include?(feature.to_s)
    end

    def ==(other)
      other && other.is_a?(Node) && (@name == other.name)
    end
    alias eql? ==

    def use_new_info?
      @use_new_info.value
    end

    def hash
      @name.hash
    end

    def inspect
      "#<Aerospike::Node: @name=#{@name}, @host=#{@host}>"
    end

    private

    def close_connections
      # drain connections and close all of them
      # non-blocking, does not call create_block when passed false
      while conn = @connections.poll(false)
        conn.close if conn
      end
    end

    # Sets node aliases
    def set_aliases(aliases)
      @aliases.value = aliases
    end

    def verify_node_name_and_cluster_name(info_map)
      info_name = info_map['node']

      if !info_name
        decrease_health
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_NODE_ERROR, "Node name is empty")
      end

      if !(@name == info_name)
        # Set node to inactive immediately.
        @active.update { |_| false }
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_NODE_ERROR, "Node name has changed. Old=#{@name} New= #{info_name}")
      end

      if cluster_name && cluster_name != info_map['cluster-name']
        @active.update { |_| false }
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_NODE_ERROR, "Cluster name does not match. expected: #{cluster_name}, got: #{info_map['cluster-name']}")
      end
    end

    def add_friends(info_map)
      friend_string = info_map['services']

      if friend_string.to_s.empty?
        @peers_count.value = 0
        return
      end

      friend_names = friend_string.split(';')
      @peers_count.value = friend_names.size

      friend_names.each do |friend|
        hostname, port = friend.split(':')
        host = Host.new(hostname, port.to_i)
        node = @cluster.find_alias(host)

        if node
          node.reference_count.update { |v| v + 1 }
        else
          unless peers.hosts.any? {|h| h == host}
            prepare_friend(host, peers)
          end
        end
      end
    end
  end # class Node
end # module
