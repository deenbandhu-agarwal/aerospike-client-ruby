# frozen_string_literal: true

# Copyright 2014-2017 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike
  class Socket
    include ::Socket::Constants

    attr_reader :socket, :timeout, :family

    def initialize(family, timeout)
      @family, @timeout = family, timeout
      @socket = ::Socket.new(family, SOCK_STREAM, 0)
      # TODO: set_keepalive_opts(@socket)
    end

    def connect
      begin
        connect!
      rescue IO::WaitWritable, Errno::EINPROGRESS
        # Block until the socket is ready, then try again
        IO.select(nil, [@socket], nil, timeout.to_f)
        begin
          connect!
        rescue Errno::EISCONN
        rescue => e
          close
          raise e
        end
      end
    end

    def connect!
      fail 'Not implemented'
    end

    def write(buffer, length)
      total = 0
      begin
        while total < length
          written = @socket.write_nonblock(buffer.read(total, length - total))
          total += written
        end
      rescue IO::WaitWritable, Errno::EAGAIN
        IO.select(nil, [@socket])
        retry
      rescue => e
        raise Aerospike::Exceptions::Connection.new("#{e}")
      end
    end

    def read(buffer, length)
      total = 0
      begin
        while total < length
          bytes = @socket.read_nonblock(length - total)
          if bytes.bytesize > 0
            buffer.write_binary(bytes, total)
          else
            # connection is dead; return an error
            raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_NOT_AVAILABLE, "Connection to the server node is dead.")
          end
          total += bytes.bytesize
        end
      rescue IO::WaitReadable,  Errno::EAGAIN
        IO.select([@socket], nil)
        retry
      rescue => e
        raise Aerospike::Exceptions::Connection.new("#{e}")
      end
    end

    def connected?
      !@socket.nil?
    end

    def valid?
      !@socket.nil?
    end

    def close
      @socket.close if @socket
      @socket = nil
    end

    def set_keepalive_opts(sock)
      sock.setsockopt(SOL_SOCKET, SO_KEEPALIVE, true)
      set_option(sock, :TCP_KEEPINTVL, DEFAULT_TCP_KEEPINTVL)
      set_option(sock, :TCP_KEEPCNT, DEFAULT_TCP_KEEPCNT)
      set_option(sock, :TCP_KEEPIDLE, DEFAULT_TCP_KEEPIDLE)
    rescue
    end

    def timeout=(timeout)
      if timeout > 0 && timeout != @timeout
        @timeout = timeout
        if IO.select([@socket], [@socket], [@socket], timeout.to_f)
          begin
            # Verify there is now a good connection
            connect!
          rescue Errno::EISCONN
            # operation successful
          rescue => e
            # An unexpected exception was raised - the connection is no good.
            close
            raise Aerospike::Exceptions::Connection.new("#{e}")
          end
        end
      end
    end
  end
end
