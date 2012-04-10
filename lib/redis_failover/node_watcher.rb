module RedisFailover
  # Watches a specific redis node for its reachability.
  class NodeWatcher
    def initialize(manager, node)
      @manager = manager
      @node = node
      @monitor_thread = nil
      @done = false
    end

    def watch
      @monitor_thread = Thread.new { monitor_node }
      self
    end

    def shutdown
      @done = true
      @node.stop_waiting
      @monitor_thread.join if @monitor_thread
    rescue
      # best effort
    end

    private

    def monitor_node
      return if @done
      @manager.notify_state_change(@node) if @node.reachable?
      @node.wait_until_unreachable
    rescue NodeUnreachableError
      @manager.notify_state_change(@node)
      relax && retry
    end

    def relax
      sleep(3)
    end
  end
end
