# frozen_string_literal: true

module Aerospike
  class Node
    module Verify
      # Fetch and set partition generation. If partitions needs to be refreshed
      # this will be indicated in node.partition_changed
      module PartitionGeneration
        class << self
          def call(node, info_map, peers)
            gen_string = info_map.fetch('partition-generation')

            raise Aerospike::Exceptions::Parse.new('partition-generation is empty') if gen_string.to_s.empty?

            generation = gen_string.to_i

            if node.partition_generation.value != generation
              Aerospike.logger.info("Node #{node.get_name} partition generation #{generation} changed")
              node.partition_changed.value = true
              node.partition_generation.value = generation
            end
          end
        end
      end
    end
  end
end