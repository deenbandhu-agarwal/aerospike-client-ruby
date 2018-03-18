# frozen_string_literal: true

require 'openssl'

module Aerospike
  module Socket
    class SSL < ::OpenSSL::SSL::SSLSocket
      include Base

      SSL_PARAMS = %i[ca_file ca_path].freeze

      class << self
        def connect(host, port, timeout, tls_name, ssl_options)
          Aerospike.logger.debug("Connecting to #{host}:#{tls_name}:#{port} using SSL options #{ssl_options}")
          tcp_sock = TCP.connect(host, port, timeout)

          ctx = ssl_options[:context] || create_context(ssl_options)

          new(tcp_sock, ctx).tap do |ssl_sock|
            ssl_sock.hostname = tls_name
            ssl_sock.connect
            ssl_sock.post_connection_check(tls_name)
          end
        end

        def create_context(ssl_options)
          OpenSSL::SSL::SSLContext.new.tap do |ctx|
            if ssl_options[:cert_file] && ssl_options[:pkey_file]
              cert = OpenSSL::X509::Certificate.new(File.read(ssl_options[:cert_file]))
              pkey = OpenSSL::PKey.read(File.read(ssl_options[:pkey_file]), ssl_options[:pkey_pass])
              if ctx.respond_to?(:add_certificate)
                ctx.add_certificate(cert, pkey)
              else
                ctx.cert = cert
                ctx.key = pkey
              end
            end

            params = ssl_options.select { |key| SSL_PARAMS.include?(key) }
            ctx.set_params(params)
          end
        end
      end
    end
  end
end
