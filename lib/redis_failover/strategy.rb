module RedisFailover
  # Loads various strategies for determining node availability.
  module Strategy
    # Loads a strategy based on the given name.
    #
    # @param [String, Symbol] name the strategy name
    # @return [Object] a new strategy instance
    def self.for(name)
      require "redis_failover/strategy/#{name.downcase}"
      const_get(name.capitalize).new
    rescue LoadError, NameError
      raise "Unknown redis failover strategy: #{name}"
    end
  end
end
