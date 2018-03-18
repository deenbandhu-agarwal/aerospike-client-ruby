# frozen_string_literal: true

module Aerospike
  class Node
    module Verify
      # Fetch and set partition generation. If partitions needs to be refreshed
      # this will be indicated in node.partition_changed
      module PartitionGeneration
        class << self
          def call(node, info_map)
            gen_string = info_map.fetch('partition-generation')

            raise Aerospike::Exceptions::Parse.new('partition-generation is empty') if gen_string.to_s.empty?

            generation = gen_string.to_i

            node.partition_generation.update(generation)

            return unless node.partition_generation.changed?
            Aerospike.logger.info("Node #{node.name} partition generation #{generation} changed")
          end
        end
      end
    end
  end
end