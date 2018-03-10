# frozen_string_literal: true

module Aerospike
  class Peers
    module Fetch
      class << self
        def call(cluster, conn)
          cmd = cluster.tls_enabled? ? 'peers-tls-std' : 'peers-clear-std'

          response = Info.request(conn, cmd)

          raise if response.size.zero?

          ::Aerospike::Peers::Parse.(response.fetch(cmd))
        end
      end
    end
  end
end
