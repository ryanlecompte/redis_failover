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
      @shutdown_lock = Mutex.new
      @shutdown_cv = ConditionVariable.new
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
      @shutdown_lock.synchronize do
        @done = true
        begin
          @node.wakeup
        rescue
          # best effort
        end
        @shutdown_cv.wait(@shutdown_lock)
      end
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
          latency = Benchmark.realtime { @node.ping }
          failures = 0
          notify(:available, latency)
          @node.wait
        rescue NodeUnavailableError => ex
          logger.debug("Failed to communicate with node #{@node}: #{ex.inspect}")
          failures += 1
          if failures >= @max_failures
            notify(:unavailable)
            failures = 0
          end
        rescue Exception => ex
          logger.error("Unexpected error while monitoring node #{@node}: #{ex.inspect}")
          logger.error(ex.backtrace.join("\n"))
        end
      end

      @shutdown_lock.synchronize do
        @shutdown_cv.broadcast
      end
    end

    # Notifies the manager of a node's state.
    #
    # @param [Symbol] state the node's state
    # @param [Integer] latency an optional latency
    def notify(state, latency = nil)
      @manager.notify_state(@node, state, latency)
    end
  end
end
