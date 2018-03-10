# frozen_string_literal: true

module Aerospike
  class Peers
    attr_accessor :peers, :hosts, :nodes, :refresh_count, :use_peers, :generation_changed

    def initialize
      @peers = ::Array.new
      @hosts = ::Set.new
      @nodes = {}
      @use_peers = true
      @refresh_count = 0
    end

    def generation_changed?
      @generation_changed == true
    end

    def use_peers?
      @use_peers == true
    end
  end
end
