module RedisFailover
  # Represents a snapshot of a particular node as seen by all currently running
  # redis node managers.
  class NodeSnapshot
    include Util

    # @return [String] the redis node
    attr_reader :node

    # Creates a new instance.
    #
    # @param [String] the redis node
    def initialize(node)
      @node = node
      @reachable = []
      @unreachable = []
    end

    # Declares this node reachable by the specified node manager.
    #
    # @param [String] the znode path for the node manager
    def reachable_by(node_manager)
      @reachable << node_manager
    end

    # Declares this node unreachable by the specified node manager.
    #
    # @param [String] the znode path for the node manager
    def unreachable_by(node_manager)
      @unreachable << node_manager
    end

    # @return [Integer] the number of node managers saying this node is reachable
    def reachable_count
      @reachable.size
    end

    # @return [Integer] the number of node managers saying this node is unreachable
    def unreachable_count
      @unreachable.size
    end

    # @return [String] a friendly representation of this node snapshot
    def to_s
      'Node %s reachable by %p, unreachable by %p (%d up, %d down)' %
        [node, @reachable, @unreachable, reachable_count, unreachable_count]
    end
  end
end
