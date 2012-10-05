module RedisFailover
  # Loads various strategies for determining which node is used during failover.
  module FailoverStrategy
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
  end
end
