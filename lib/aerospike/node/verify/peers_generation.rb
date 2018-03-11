# frozen_string_literal: true

module Aerospike
  class Node
    module Verify
      module PeersGeneration
        class << self
          def call(node, info_map, peers)
            gen_string = info_map.fetch('peers-generation')

            raise Aerospike::Exceptions::Parse.new('peers-generation is empty') if gen_string.to_s.empty?

            generation = gen_string.to_i

            if node.peers_generation.value != generation
              Aerospike.logger.info("Node #{node.get_name} peers generation #{generation} changed")
              peers.generation_changed = true
            end
          end
        end
      end
    end
  end
end