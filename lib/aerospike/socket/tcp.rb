require 'socket'

module Aerospike
  module Socket
    class TCP < Connection
      def initialize(host, port, timeout)
        @host, @port, @timeout = host, port, timeout
        @socket = ::Socket.new(AF_INET, SOCK_STREAM, 0)
        connect
      end

      def connect!
        @socket.connect_nonblock(::Socket.sockaddr_in(port, host))
        self
      end
    end
  end
end
