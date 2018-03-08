# frozen_string_literal: true

require 'socket'

module Aerospike
  class Socket
    class TCP < Socket
      attr_reader :host, :port
      def initialize(host, port, timeout)
        @host, @port = host, port
        super(AF_INET, timeout)
      end

      def connect!
        @socket.connect_nonblock(::Socket.sockaddr_in(port, host))
      end
    end
  end
end
