# frozen_string_literal: true

module Aerospike
  class Node
    module CreateConnection
      class << self
        def call(node)
          ::Aerospike::Connection::Create.(
            node.host.name, node.host.port,
            tls_name: node.host.tls_name,
            timeout: node.cluster.connection_timeout,
            ssl_options: node.cluster.ssl_options
          )
        end
      end
    end
  end
end
