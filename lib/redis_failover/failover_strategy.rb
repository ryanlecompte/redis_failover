module RedisFailover
  # Base class for strategies that determine which node is used during failover.
  class FailoverStrategy
    # Loads a strategy based on the given name.
    #
    # @param [String, Symbol] name the strategy name
    # @return [Object] a new strategy instance
    def self.for(name)
      require "redis_failover/failover_strategy/#{name.downcase}"
      const_get(name.capitalize).new
    rescue LoadError, NameError
      raise "Failed to find failover strategy: #{name}"
    end

    # Returns a candidate node as determined by this strategy.
    #
    # @param [Hash<Node, NodeSnapshot>] snapshots the node snapshots
    # @return [Node] the candidate node or nil if one couldn't be found
    def find_candidate(snapshots)
      raise NotImplementedError
    end
  end
end
