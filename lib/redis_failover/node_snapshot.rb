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
      @state = {}
    end

    # Creates a report based on lag and latency collected before
    #
    # @param [String] node_manager: the node manager id
    # @param [Integer] lag: sync latency between master and slave
    # @param [Integer] latency: network latency between master and slave
    def update_state(node_manager, lag, latency)
      @state[node_manager] = {}
      @state[node_manager][:lag] = lag
      @state[node_manager][:latency] = latency
      @state[node_manager][:state] = state_from_report(@state[node_manager])
    end

    def update_state_from_report(node_manager, report)
      update_state(node_manager, report[:lag], report[:latency])
    end

    # Declares this node available by the specified node manager.
    #
    # @param [String] node_manager the node manager id
    # @param [Integer] latency the latency
    def state_from_report(node_manager_report)
      if node_manager_report[:latency] && node_manager_report[:latency] >= 0
        :available
      else
        :unavailable
      end
    end

    # Determines if this node is viewable by a node manager.
    #
    # @param [String] node_manager the node manager id
    def viewable_by?(node_manager)
      node_manager_view = @state[node_manager]
      node_manager_view && node_manager_view[:lag].to_i >= 0
    end

    # @return [Integer] the number of node managers saying
    # this node is available
    def available_nodes
      @state.map {|node_manager, report| node_manager if report[:latency].to_i >= 0}.compact
    end

    # @return [Integer] the number of node managers saying
    # this node is electable
    def electable_nodes
      @state.map {|node_manager, report| node_manager if report[:lag].to_i >= 0}.compact
    end

    # @return [Integer] the number of node managers saying
    # this node is unavailable
    def unavailable_nodes
      @state.map {|node_manager, report| node_manager if report[:latency].to_i < 0}.compact
    end

    # @return [Integer] the number of node managers saying
    # this node is available
    def available_count
      available_nodes.size
    end

    # @return [Integer] the number of node managers saying
    # this node is unavailable
    def unavailable_count
      unavailable_nodes.size
    end

    # @return [Integer] the average available latency
    def avg_lag
      return if @state.empty?
      @state.values.inject(0) { |sum, report| sum + report[:lag].to_i } / @state.size
    end

    # @return [Integer] the average available latency
    def avg_latency
      return if @state.empty?
      @state.values.inject(0) { |sum, report| sum + report[:latency].to_i } / @state.size
    end

    # @return [Array<String>] all node managers involved in this snapshot
    def node_managers
      @state.keys
    end

    # @return [Boolean] true if all node managers indicated that this
    # node was viewable
    def all_available?
      available_count == @state.size
    end

    # @return [Boolean] true if all node managers indicated that this
    # node was viewable
    def all_electable?
      electable_nodes.size == @state.size
    end

    # @return [Boolean] true if all node managers indicated that this
    # node was unviewable
    def all_unavailable?
      unavailable_count == @state.size
    end

    # @return [String] a friendly representation of this node snapshot
    def to_s
      'Node %s available by %p, unavailable by %p (%d up, %d down)' %
        [node, available_nodes, unavailable_nodes, available_count, unavailable_count]
    end
  end
end
