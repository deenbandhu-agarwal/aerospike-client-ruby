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
  module Connection # :nodoc:
    module Create
      class << self
        def call(host, port, timeout = 30, ssl_options = {})
          if !ssl_options.nil? && ssl_options[:enable] == true
            ::Aerospike::Socket::SSL.new(host, port, timeout, ssl_options)
          else
            ::Aerospike::Socket::TCP.new(host, port, timeout)
          end.tap do |conn|
            conn.connect
          end
        end
      end
    end
  end
end
