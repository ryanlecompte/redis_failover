module RedisFailover
  class Error < StandardError
    attr_reader :original
    def initialize(msg = nil, original = $!)
      super(msg)
      @original = original
    end
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

  class InvalidNodeRoleError < Error
    def initialize(node, assumed, actual)
      super("Invalid role detected for node #{node}, client thought " +
        "it was a #{assumed}, but it's now a #{actual}")
    end
  end

  class UnsupportedOperationError < Error
    def initialize(operation)
      super("Operation `#{operation}` is currently unsupported")
    end
  end

  class MissingNodeManagerError < Error
    def initialize(timeout)
      super("Failed to hear from Node Manager within #{timeout} seconds")
    end
  end
end