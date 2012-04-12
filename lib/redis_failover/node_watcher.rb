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
      @node.ping
      @manager.notify_state_change(@node, :reachable)
      @node.wait_until_unreachable
    rescue NodeUnreachableError
      @manager.notify_state_change(@node, :unreachable)
      sleep(3) && retry
    end
  end
end
