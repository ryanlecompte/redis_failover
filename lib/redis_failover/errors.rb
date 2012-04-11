module RedisFailover
  class Error < StandardError
  end

  class InvalidNodeError < Error
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
end