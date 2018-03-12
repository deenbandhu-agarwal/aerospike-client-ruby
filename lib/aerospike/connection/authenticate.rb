# frozen_string_literal: true

module Aerospike
  module Connection # :nodoc:
    module Authenticate
      class << self
        def call(conn, user, password)
          command = AdminCommand.new
          command.authenticate(conn, @cluster.user, @cluster.password)
          true
        rescue ::Aerospike::Exceptions::Aerospike
          conn.close if conn
          raise ::Aerospike::Exceptions::InvalidCredentials
        end
      end
    end
  end
end
