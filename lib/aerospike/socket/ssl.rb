# frozen_string_literal: true

require 'openssl'

module Aerospike
  module Socket
    class SSL < ::OpenSSL::SSL::SSLSocket

      include Base

      def self.connect(host, port, timeout, tls_name, ssl_options)
        tcp_sock = TCP.connect(host, port, timeout)

        ctx = OpenSSL::SSL::SSLContext.new
        ctx.set_params(ssl_options) if ssl_options && !ssl_options.empty?

        ssl_sock = new(tcp_sock, ctx)
        ssl_sock.hostname = tls_name
        ssl_sock.connect
        ssl_sock.post_connection_check(host)

        ssl_sock
      end
    end
  end
end
