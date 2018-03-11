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

    attr_reader :reference_count, :responded, :name, :features, :cluster_name, :partition_changed, :partition_generation, :peers_generation, :failures, :cluster, :peers_count

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
      @failures = Atomic.new(0)

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

    # Sets node as active
    def active!
      @active.update { |_| true }
    end

    # Sets node as inactive
    def inactive!
      @active.update { |_| false }
    end

    # Checks if the node is active
    def active?
      @active.value
    end

    def responded!
      @responded.value = true
    end

    def reset_failures!
      @failures.value = 0
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
  end # class Node
end # module
