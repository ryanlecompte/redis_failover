module RedisFailover
  class Error < StandardError
  end

  class InvalidNodeError < Error
  end

  class InvalidNodeStateError < Error
    def initialize(node, state)
      super("Invalid state change `#{state}` for node #{node}")
    end
  end

  class NodeUnavailableError < Error
    def initialize(node)
      super("Node: #{node}")
    end
  end

  class NoMasterError < Error
  end

  class NoSlaveError < Error
  end

  class FailoverServerUnavailableError < Error
    def initialize(failover_server_url)
      super("Unable to access #{failover_server_url}")
    end
  end

  class InvalidNodeRoleError < Error
    def initialize(node, assumed, actual)
      super("Invalid role detected for node #{node}, client thought " +
        "it was a #{assumed}, but it's now a #{actual}")
    end
  end
end