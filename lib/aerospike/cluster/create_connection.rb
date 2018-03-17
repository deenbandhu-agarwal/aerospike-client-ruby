# frozen_string_literal: true

module Aerospike
  class Cluster
    module CreateConnection
      class << self
        def call(cluster, host)
          ::Aerospike::Connection::Create.(
            host.name,
            host.port,
            tls_name: host.tls_name,
            timeout: cluster.connection_timeout,
            ssl_options: cluster.ssl_options
          )
        end
      end
    end
  end
end
