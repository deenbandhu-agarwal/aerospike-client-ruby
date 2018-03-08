# frozen_string_literal: true

module Aerospike
  class Cluster
    class Peer
      module Parse
        Object = ::Struct.new(:generation, :port_default, :peers)
        class << self

          BASE_REGEX = /(\d+),(\d*),\[(.*)\]/.freeze

          def call(response)
            gen, port, peers = BASE_REGEX.match(response).to_a.last(3)

            ::Aerospike::Cluster::Peer::Parse::Object.new.tap do |obj|
              obj.generation = gen.to_i
              obj.port_default = port.empty? ? nil : port.to_i
              obj.peers = parse_peers(peers)
            end
          end

          def parse_peers(response)
            return [] if response.empty?
            parser = ::Aerospike::Utils::StringParser.new(response)
            [].tap do |result|
              loop do
                result << parse_peer(parser)
                break unless parser.current == ','
                parser.step
              end
            end
          end

          def parse_peer(parser)
            ::Aerospike::Cluster::Peer.new.tap do |peer|
              parser.expect('[')
              peer.node_name = parser.read_until(',')
              peer.tls_name = parser.read_until(',')
              peer.hosts = parse_hosts(parser, peer)
              parser.expect(']')
            end
          end

          def parse_hosts(parser, peer)
            parser.expect('[')
            return [] if parser.current == ']'

            [].tap do |result|
              loop do
                result << parse_host(parser, peer)
                break unless parser.current == ','
                parser.step
              end
            end
          end

          def parse_host(parser, peer)
            # TODO: handle IPv6
            parser.read_until(']').split(',').map do |host|
              hostname, port = host.split(':')
              ::Aerospike::Host.new(hostname, port, peer.tls_name)
            end
          end
        end
      end
    end
  end
end