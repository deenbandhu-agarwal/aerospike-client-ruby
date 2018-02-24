require 'socket'

module Aerospike
  module Socket
    class TCP
      attr_reader :socket, :host, :port, :timeout

      def initialize(host, port, timeout)
        @host = host
        @port = port
        @timeout = timeout
        @socket = ::Socket.new(::Socket::AF_INET, ::Socket::SOCK_STREAM, 0)
      end

      def connect_nonblock
        @socket.connect_nonblock(::Socket.sockaddr_in(port, host))
      end

      def close
        @socket.close
      end

      def recv_nonblock(maxlen)
        @socket.recv_nonblock(maxlen)
      end

      def write_nonblock(data)
        @socket.write_nonblock(data)
      end
    end
  end
end