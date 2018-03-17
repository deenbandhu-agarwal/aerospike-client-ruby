# frozen_string_literal: true

module Aerospike
  class Host
    module Parse
      INTEGER_REGEX = /\A\d+\z/

      class << self
        # Parse hosts from string format: hostname1[:tlsname1][:port1],...
        def call(hosts, default_port = 3000)
          case hosts
          when Host
            [hosts]
          when Array
            hosts
          when String
            hosts.split(?,).map { |host|
              addr, tls_name, port = host.split(?:)
              if port.nil? && tls_name && tls_name.match?(INTEGER_REGEX)
                port = tls_name
                tls_name = nil
              end
              port ||= default_port
              Host.new(addr, port.to_i, tls_name)
            }
          else
            fail TypeError, "hosts should be a Host object, an Array of Host objects, or a String"
          end
        end
      end
    end
  end
end