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
      @available = []
      @unavailable = []
    end

    # Declares this node available by the specified node manager.
    #
    # @param [String] the znode path for the node manager
    def viewable_by(node_manager)
      @available << node_manager
    end

    # Declares this node unavailable by the specified node manager.
    #
    # @param [String] the znode path for the node manager
    def unviewable_by(node_manager)
      @unavailable << node_manager
    end

    # @return [Integer] the number of node managers saying
    # this node is available
    def available_count
      @available.size
    end

    # @return [Integer] the number of node managers saying
    # this node is unavailable
    def unavailable_count
      @unavailable.size
    end

    # @return [Symbol] the node state as determined by the
    # majority node managers
    def majority_state
      available_count > unavailable_count ? :available : :unavailable
    end

    # @return [String] a friendly representation of this node snapshot
    def to_s
      'Node %s available by %p, unavailable by %p (%d up, %d down)' %
        [node, @available, @unavailable, available_count, unavailable_count]
    end
  end
end
