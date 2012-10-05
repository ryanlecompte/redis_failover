module RedisFailover
  # Base class for strategies that determine node availability.
  class NodeStrategy
    include Util

    # Loads a strategy based on the given name.
    #
    # @param [String, Symbol] name the strategy name
    # @return [Object] a new strategy instance
    def self.for(name)
      require "redis_failover/node_strategy/#{name.downcase}"
      const_get(name.capitalize).new
    rescue LoadError, NameError
      raise "Failed to find node strategy: #{name}"
    end

    # Returns the state determined by this strategy.
    #
    # @param [Node] the node to handle
    # @param [Hash<Node, NodeSnapshot>] snapshots the current set of snapshots
    # @return [Symbol] the status
    def determine_state(node, snapshots)
      raise NotImplementedError
    end

    # Logs a node as being unavailable.
    #
    # @param [Node] node the node
    # @param [NodeSnapshot] snapshot the node snapshot
    def log_unavailable(node, snapshot)
      logger.info("#{self.class} marking #{node} as unavailable. Snapshot: #{snapshot}")
    end
  end
end
