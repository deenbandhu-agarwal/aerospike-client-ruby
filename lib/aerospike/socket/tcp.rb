# frozen_string_literal: true

require 'socket'

module Aerospike
  module Socket
    class TCP < ::Socket
      include Base

      def self.connect(host, port, timeout)
        Aerospike.logger.debug("Trying to connect to #{host}:#{port} with #{timeout}s timeout")
        sock = new(::Socket::AF_INET, ::Socket::SOCK_STREAM, 0)
        sockaddr = ::Socket.sockaddr_in(port, host)

        begin
          sock.connect_nonblock(sockaddr)
        rescue IO::WaitWritable, Errno::EINPROGRESS
          ::IO.select(nil, [sock], nil, timeout)

          # Because IO.select behaves (return values are different) differently on
          # different rubies, lets just try `connect_noblock` again. An exception
          # is raised to indicate the current state of the connection, and at this
          # point, we are ready to decide if this is a success or a timeout.
          begin
            sock.connect_nonblock(sockaddr)
          rescue Errno::EISCONN
            # Good, we're connected.
          rescue Errno::EINPROGRESS, Errno::EALREADY
            # Bad, we're still waiting to connect.
            raise ::Aerospike::Exceptions::Connection, "Connection attempt to #{host}:#{port} timed out after #{timeout} secs"
          rescue => e
            raise ::Aerospike::Exceptions::Connection, e.message
          end
        end

        sock
      end
    end
  end
end
