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

  class NodeUnreachableError < Error
    def initialize(node)
      super("Node: #{node}")
    end
  end

  class NoMasterError < Error
  end

  class NoSlaveError < Error
  end

  class FailoverServerUnreachableError < Error
    def initialize(failover_server_url)
      super("Unable to reach #{failover_server_url}")
    end
  end
end