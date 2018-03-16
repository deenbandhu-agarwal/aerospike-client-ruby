# frozen_string_literal: true

require 'openssl'

module Aerospike
  module Socket
    class SSL < ::OpenSSL::SSL::SSLSocket
      include Base

      def self.connect(host, port, timeout, tls_name, ssl_options)
        Aerospike.logger.debug("Connecting to #{host}:#{tls_name}:#{port} using SSL options #{ssl_options}")
        tcp_sock = TCP.connect(host, port, timeout)

        ctx = ssl_options[:context] || new_context(ssl_options)

        ssl_sock = new(tcp_sock, ctx)
        ssl_sock.hostname = tls_name
        ssl_sock.connect
        ssl_sock.post_connection_check(tls_name)

        ssl_sock
      end

      def self.new_context(ssl_options)
          OpenSSL::SSL::SSLContext.new.tap do |ctx|
            if ssl_options[:cert_file] && ssl_options[:pkey_file]
              cert = OpenSSL::X509::Certificate.new(File.read(ssl_options[:cert_file]))
              pkey = OpenSSL::PKey.read(File.read(ssl_options[:pkey_file]), ssl_options[:pkey_pass])
              ctx.add_certificate(cert, pkey)
            end
            ctx.set_params(ssl_options.slice(:ca_file, :ca_path))
          end
      end
    end
  end
end
