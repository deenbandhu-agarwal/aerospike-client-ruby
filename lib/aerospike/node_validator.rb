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

module Aerospike
  class NodeValidator # :nodoc:
    VERSION_REGEXP = /(?<v1>\d+)\.(?<v2>\d+)\.(?<v3>\d+).*/.freeze

    attr_reader :host, :aliases, :name, :use_new_info, :features, :cluster_name, :ssl_options, :conn

    def initialize(cluster, host, timeout, cluster_name, ssl_options = {})
      @cluster = cluster
      @use_new_info = true
      @features = Set.new
      @host = host
      @cluster_name = cluster_name
      @ssl_options = ssl_options

      set_aliases(host)
      set_address(timeout)
    end

    def set_aliases(host)
      is_ip = !!((host =~ Resolv::IPv4::Regex) || (host =~ Resolv::IPv6::Regex))

      addresses = if is_ip
                    # Don't try to resolve IP addresses.
                    # May fail in different OS or network setups
                     host
                  else
                    Resolv.getaddresses(host.name)
      end

      @aliases = addresses.map { |addr| Host.new(addr, host.port, host.tls_name) }

      Aerospike.logger.debug("Node Validator has #{aliases.length} nodes.")
    end

    def set_address(timeout)
      @aliases.each do |aliass|
        begin
          conn = Cluster::CreateConnection.(@cluster, @host)

          info_map = Info.request(conn, 'node', 'build', 'features')
          if node_name = info_map['node']
            @name = node_name

            # Set features
            if features = info_map['features']
              @features = features.split(';').to_set
            end

            # Check new info protocol support for >= 2.6.6 build
            if build_version = info_map['build']
              v1, v2, v3 = parse_version_string(build_version)
              @use_new_info = v1.to_i > 2 || (v1.to_i == 2 && (v2.to_i > 6 || (v2.to_i == 6 && v3.to_i >= 6)))
            end
          end
        ensure
          conn.close if conn
        end
      end
    end

    protected

    def parse_version_string(version)
      if v = VERSION_REGEXP.match(version)
        return v['v1'], v['v2'], v['v3']
      end

      raise Aerospike::Exceptions::Parse.new("Invalid build version string in Info: #{version}")
    end
  end # class
end #module
