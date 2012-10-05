module RedisFailover
  # Represents a snapshot of a particular redis node as seen by all currently running
  # redis node managers.
  class NodeSnapshot
    # @return [String] the redis node
    attr_reader :node

    # Creates a new instance.
    #
    # @param [String] the redis node
    # @see NodeManager#initialize
    def initialize(node)
      @node = node
      @available = {}
      @unavailable = []
    end

    # Declares this node available by the specified node manager.
    #
    # @param [String] node_manager the znode path for the node manager
    # @param [Integer] latency the latency
    def viewable_by(node_manager, latency)
      @available[node_manager] = latency
    end

    # Declares this node unavailable by the specified node manager.
    #
    # @param [String] node_manager the znode path for the node manager
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

    # @return [Integer] the average available latency
    def avg_latency
      return if @available.empty?
      @available.values.inject(0) { |sum, n| sum + n } / @available.size
    end

    # @return [Array<String>] all node managers involved in this snapshot
    def node_managers
      (@available.keys + @unavailable).uniq
    end

    # @return [Boolean] true if all node managers indicated that this
    # node was viewable
    def all_available?
      available_count > 0 && unavailable_count == 0
    end

    # @return [Boolean] true if all node managers indicated that this
    # node was unviewable
    def all_unavailable?
      unavailable_count > 0 && available_count == 0
    end

    # @return [String] a friendly representation of this node snapshot
    def to_s
      'Node %s available by %p, unavailable by %p (%d up, %d down)' %
        [node, @available, @unavailable, available_count, unavailable_count]
    end
  end
end
