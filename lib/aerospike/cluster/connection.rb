# encoding: utf-8
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

  private

  class Connection # :nodoc:

    def initialize(host, port, timeout = 30, ssl_options = {})
      @socket = connect(host, port, timeout, ssl_options)
      @timeout = timeout
    end

    def connect(host, port, timeout, ssl_options = {})
      socket = if !ssl_options.nil? && ssl_options[:enable] == true
        ::Aerospike::Socket::SSL.new(host, port, timeout, ssl_options)
      else
        ::Aerospike::Socket::TCP.new(host, port, timeout)
      end

      begin
        socket.connect!
      rescue IO::WaitWritable, Errno::EINPROGRESS
        # Block until the socket is ready, then try again
        IO.select(nil, [socket.socket], nil, timeout.to_f)
        begin
          socket.connect!
        rescue Errno::EISCONN
        rescue => e
          socket.close
          raise e
        end
        socket
      end
    end

    def write(buffer, length)
      total = 0
      while total < length
        begin
          written = @socket.write(buffer.read(total, length - total))
          total += written
        rescue IO::WaitWritable, Errno::EAGAIN
          IO.select(nil, [@socket.socket])
          retry
        rescue => e
          raise Aerospike::Exceptions::Connection.new("#{e}")
        end
      end
    end

    def read(buffer, length)
      total = 0
      while total < length
        begin
          bytes = @socket.read(length - total)
          if bytes.bytesize > 0
            buffer.write_binary(bytes, total)
          else
            # connection is dead; return an error
            raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_NOT_AVAILABLE, "Connection to the server node is dead.")
          end
          total += bytes.bytesize
        rescue IO::WaitReadable,  Errno::EAGAIN
          IO.select([@socket.socket], nil)
          retry
        rescue => e
          raise Aerospike::Exceptions::Connection.new("#{e}")
        end
      end
    end

    def connected?
      @socket != nil
    end

    def valid?
      @socket != nil
    end

    def close
      @socket.close if @socket
      @socket = nil
    end

    def timeout=(timeout)
      if timeout > 0 && timeout != @timeout
        @timeout = timeout
        if IO.select([@socket.socket], [@socket.socket], [@socket.socket], timeout.to_f)
          begin
            # Verify there is now a good connection
            @socket.connect!
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
