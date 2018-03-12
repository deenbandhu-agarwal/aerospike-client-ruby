# frozen_string_literal: true

require 'socket'

module Aerospike
  module Socket
    class TCP < ::Socket
      include Base

      def self.connect(host, port, timeout)
        sock = new(::Socket::AF_INET, ::Socket::SOCK_STREAM, 0)
        sockaddr = ::Socket.sockaddr_in(port, host)

        begin
          sock.connect_nonblock(sockaddr)
        rescue IO::WaitWritable, Errno::EINPROGRESS
          ::IO.select(nil, [sock], nil, timeout)

          begin
            sock.connect_nonblock(sockaddr)
          rescue Errno::EISCONN
          rescue => e
            raise e
          end
        end

        sock
      end
    end
  end
end
