module RedisFailover
  # Watches a specific redis node for its availability.
  class NodeWatcher
    include Util

    WATCHER_SLEEP_TIME = 3

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

      loop do
        begin
          return if @done
          sleep(WATCHER_SLEEP_TIME)
          failures = 0
          @node.ping

          if @node.syncing_with_master? && @node.prohibits_stale_reads?
            notify_unavailable
            logger.info("Node #{to_s} not ready yet, still syncing with master.")
          else
            notify_available
            @node.wait
          end
        rescue NodeUnavailableError
          failures += 1
          if failures >= @max_failures
            notify_unavailable
            failures = 0
          end
        end
      end
    end

    def notify_available
      @manager.notify_state_change(@node, :available)
    end

    def notify_unavailable
      @manager.notify_state_change(@node, :unavailable)
    end
  end
end
