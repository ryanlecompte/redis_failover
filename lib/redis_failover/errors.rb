module RedisFailover

  module EtcdClientLock
    class LockHoldError < StandardError; end
  end
  class EtcdNoMasterError < StandardError; end

  # Base class for all RedisFailover errors.
  class Error < StandardError
  end

  # Raised when a node is specified incorrectly.
  class InvalidNodeError < Error
  end

  # Raised when a node changes to an invalid/unknown state.
  class InvalidNodeStateError < Error
    def initialize(node, state)
      super("Invalid state change `#{state}` for node #{node}")
    end
  end

  # Raised when a node is unavailable (i.e., unreachable via network).
  class NodeUnavailableError < Error
    def initialize(node)
      super("Node: #{node}")
    end
  end

  # Raised when no master is currently available.
  class NoMasterError < Error
  end

  # Raised when more than one master is found on startup.
  class MultipleMastersError < Error
    def initialize(nodes)
      super("Multiple nodes with master role: #{nodes.map(&:to_s)}")
    end
  end

  # Raised when no slave is currently available.
  class NoSlaveError < Error
  end

  # Raised when a redis server is no longer using the same role
  # as previously assumed.
  class InvalidNodeRoleError < Error
    def initialize(node, assumed, actual)
      super("Invalid role detected for node #{node}, client thought " +
        "it was a #{assumed}, but it's now a #{actual}")
    end
  end

  # Raised when an unsupported redis operation is performed.
  class UnsupportedOperationError < Error
    def initialize(operation)
      super("Operation `#{operation}` is currently unsupported")
    end
  end
end
