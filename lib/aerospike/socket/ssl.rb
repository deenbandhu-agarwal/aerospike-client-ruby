require 'socket'
require 'openssl'

module Aerospike
  module Socket
    class SSL
      attr_reader :socket, :context, :host, :port

      def initialize(host, port, timeout, ssl_options)
        @host, @port, @timeout = host, port, timeout
        @socket = ::Socket.new(::Socket::AF_INET, ::Socket::SOCK_STREAM, 0)
        @context = create_context(ssl_options)
        @ssl_socket = OpenSSL::SSL::SSLSocket.new(@socket, context)
        @ssl_socket.sync_close = true
      end

      def connect!
        @socket.connect(::Socket.sockaddr_in(port, host))
        @ssl_socket.connect
        verify_certificate!(@ssl_socket)
        self
      end

      def close
        @ssl_socket.sysclose
      end

      def read(maxlen)
        @ssl_socket.sysread(maxlen)
      end

      def write(data)
        @ssl_socket.syswrite(data)
      end

      private

      def create_context(options)
        OpenSSL::SSL::SSLContext.new().tap do |ctx|
          set_cert(ctx, options)
          set_key(ctx, options)
          set_cert_verification(ctx, options)
          set_cipher_suite(ctx, options)
          set_protocols(ctx, options)
        end
      end

      def set_cert(ctx, options)
        if options[:cert_file]
          ctx.cert = OpenSSL::X509::Certificate.new(File.open(options[:cert_file]))
        end
      end

      def set_key(ctx, options)
        passphrase = options[:key_file_pass_phrase]
        if options[:key_file]
          ctx.key = passphrase ? OpenSSL::PKey.read(File.open(options[:key_file]), passphrase) :
            OpenSSL::PKey.read(File.open(options[:key_file]))
        end
      end

      def set_cert_verification(ctx, options)
        ctx.verify_mode = OpenSSL::SSL::VERIFY_PEER
        cert_store = OpenSSL::X509::Store.new
        if options[:ca_file]
          cert_store.add_cert(OpenSSL::X509::Certificate.new(File.open(options[:ca_file])))
        else
          cert_store.set_default_paths
        end
        ctx.cert_store = cert_store
      end

      def set_cipher_suite(ctx, options)
        if options[:cipher_suite]
          # TODO(wallin)
        end
      end

      def set_protocols(ctx, options)
        if options[:protocols]
          # TODO(wallin)
        end
      end

      def verify_certificate!(socket)
        return unless context.verify_mode == OpenSSL::SSL::VERIFY_PEER
        return if OpenSSL::SSL.verify_certificate_identity(
          socket.peer_cert, host
        )
        # TODO(wallin): raise correct error
        raise
      end
    end
  end
end
