# frozen_string_literal: true

module Aerospike
  class Cluster
    # Create connection based on cluster config and authenticate if needed
    module CreateConnection
      class << self
        def call(cluster, host)
          ::Aerospike::Connection::Create.(
            host.name,
            host.port,
            tls_name: host.tls_name,
            timeout: cluster.connection_timeout,
            ssl_options: cluster.ssl_options
          ).tap do |conn|
            if cluster.credentials_given?
              # Authenticate will raise and close connection if invalid credentials
              Connection::Authenticate.(conn, cluster.user, cluster.password)
            end
          end
        end
      end
    end
  end
end
