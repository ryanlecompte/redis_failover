module RedisFailover
  class Error < StandardError; end

  class InvalidNodeError < Error; end

  class NodeUnreachableError < Error; end

  class NoMasterError < Error; end
end