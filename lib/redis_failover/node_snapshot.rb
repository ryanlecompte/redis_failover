module RedisFailover
  # Represents a snapshot of a particular redis node as seen by all currently running
  # redis node managers.
  class NodeSnapshot
    # @return [String] the redis node
    attr_reader :node

    # Creates a new instance.
    #
    # @param [String] the redis node
    # @param [Symbol] decision_mode the decision mode
    # @see NodeManager#initialize
    def initialize(node, decision_mode)
      @node = node
      @decision_mode = decision_mode
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

    # @return [Boolean] true if all node managers indicated that this
    # node was viewable
    def all_available?
      available_count > 0 && unavailable_count == 0
    end

    # @return [Symbol] the node state as determined by the
    # majority node managers
    def state
      case @decision_mode
      when :majority
        available_count > unavailable_count ? :available : :unavailable
      when :consensus
        all_available? ? :available : :unavailable
      end
    end

    # @return [String] a friendly representation of this node snapshot
    def to_s
      'Node %s available by %p, unavailable by %p (%d up, %d down, %s mode)' %
        [node, @available, @unavailable, available_count, unavailable_count, @decision_mode]
    end
  end
end
