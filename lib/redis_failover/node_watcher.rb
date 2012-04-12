module RedisFailover
  # Watches a specific redis node for its reachability.
  class NodeWatcher
    def initialize(manager, node, max_failures)
      @manager = manager
      @node = node
      @max_failures = max_failures
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
      failures = 0

      begin
        return if @done
        @node.ping
        failures = 0
        @manager.notify_state_change(@node, :reachable)
        @node.wait_until_unreachable
      rescue NodeUnreachableError
        failures += 1
        if failures >= @max_failures
          @manager.notify_state_change(@node, :unreachable)
          failures = 0
        end
        sleep(3) && retry
      end
    end
  end
end
