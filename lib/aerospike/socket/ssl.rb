# frozen_string_literal: true

require 'socket'
require 'openssl'

module Aerospike
  class Socket
    class SSL < Socket
      attr_reader :context, :host, :port, :tls_name, :tcp_socket

      def initialize(host, port, timeout, tls_name, ssl_options)
        @host, @port, @timeout, @tls_name = host, port, timeout, tls_name
        # Use context from options if passed.
        ssl_options ||= {}
        @context = ssl_options[:context] || create_context(ssl_options)
        create_sockets(context)
      end

      def connect!
        @tcp_socket.connect(::Socket.sockaddr_in(port, host))
        @socket.connect
        verify_certificate!(@socket)
      end

      private

      def create_context(options)
        OpenSSL::SSL::SSLContext.new().tap do |ctx|
          set_cert(ctx, options)
          set_key(ctx, options)
          set_cert_verification(ctx, options)
          set_versions(ctx, options)
        end
      end

      def create_sockets(context)
        @tcp_socket = ::Socket.new(AF_INET, SOCK_STREAM, 0)
        @socket = OpenSSL::SSL::SSLSocket.new(@tcp_socket, context)
        @socket.sync_close = true
      end

      def set_cert(ctx, options)
        if options[:cert_file]
          ctx.cert = ::OpenSSL::X509::Certificate.new(
            File.open(options[:cert_file])
          )
        end
      end

      def set_key(ctx, options)
        passphrase = options[:key_file_pass_phrase]
        if options[:key_file]
          ctx.key = if passphrase
            ::OpenSSL::PKey.read(File.open(options[:key_file]), passphrase)
          else
            ::OpenSSL::PKey.read(File.open(options[:key_file]))
          end
        end
      end

      def set_cert_verification(ctx, options)
        ctx.verify_mode = ::OpenSSL::SSL::VERIFY_PEER
        cert_store = ::OpenSSL::X509::Store.new
        if options[:ca_file]
          cert_store.add_cert(
            ::OpenSSL::X509::Certificate.new(File.open(options[:ca_file]))
          )
        else
          cert_store.set_default_paths
        end
        ctx.cert_store = cert_store
      end

      def set_versions(ctx, options)
        # TODO(wallin)
      end

      def verify_certificate!(socket, tls_name)
        return unless context.verify_mode == ::OpenSSL::SSL::VERIFY_PEER
        return if ::OpenSSL::SSL.verify_certificate_identity(
          socket.peer_cert, tls_name
        )
        # TODO(wallin): raise correct error
        raise
      end
    end
  end
end
