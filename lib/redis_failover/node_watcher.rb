module RedisFailover
  # NodeWatcher periodically monitors a specific redis node for its availability.
  # NodeWatcher instances periodically report a redis node's current state
  # to the NodeManager for proper handling.
  class NodeWatcher
    include Util

    # Time to sleep before checking on the monitored node's status.
    WATCHER_SLEEP_TIME = 2

    # Creates a new instance.
    #
    # @param [NodeManager] manager the node manager
    # @param [Node] node the node to watch
    # @param [Integer] max_failures the max failues before reporting node as down
    def initialize(manager, node, max_failures)
      @manager = manager
      @node = node
      @max_failures = max_failures
      @monitor_thread = nil
      @done = false
    end

    # Starts the node watcher.
    #
    # @note this method returns immediately and causes monitoring to be
    #   performed in a new background thread
    def watch
      @monitor_thread ||= Thread.new { monitor_node }
      self
    end

    # Performs a graceful shutdown of this watcher.
    def shutdown
      @done = true
      @monitor_thread.join
    rescue => ex
      logger.warn("Failed to gracefully shutdown watcher for #{@node}")
    end

    private

    # Periodically monitors the redis node and reports state changes to
    # the {RedisFailover::NodeManager}.
    def monitor_node
      failures = 0

      loop do
        begin
          break if @done
          sleep(WATCHER_SLEEP_TIME)
          lag = Benchmark.realtime { @node.healthcheck }
          failures = 0
          notify(:available, lag)
        rescue NodeUnavailableError => ex
          logger.warn("Failed to communicate with node #{@node}: #{ex.inspect}")
          failures += 1
          if failures >= @max_failures
            notify(:unavailable, lag)
            failures = 0
          end
        rescue Exception => ex
          logger.error("Unexpected error while monitoring node #{@node}: #{ex.inspect}")
          logger.error(ex.backtrace.join("\n"))
        end
      end
    end

    # Notifies the manager of a node's state.
    #
    # @param [Symbol] state the node's state
    # @param [Integer] lag data sync latency
    def notify(state, lag)
      ping = @node.electability rescue -1
      state = :electable if ping > 0
      @manager.notify_state(@node, state, lag, ping)
    end
  end
end
