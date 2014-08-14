require_relative 'node_manager_impl'
module RedisFailover
  # NodeManager manages a list of redis nodes. Upon startup, the NodeManager
  # will discover the current redis master and slaves. Each redis node is
  # monitored by a NodeWatcher instance. The NodeWatchers periodically
  # report the current state of the redis node it's watching to the
  # NodeManager. The NodeManager processes the state reports and reacts
  # appropriately by handling stale/dead nodes, and promoting a new redis master
  # if it sees fit to do so.
  class ZookeeperNodeManager < NodeManagerImpl

    # Starts the node manager.
    #
    # @note This method does not return until the manager terminates.
    def start
      return unless running?
      setup_zk
      spawn_watchers
      wait_until_master
    rescue *ZK_ERRORS => ex
      logger.error("ZK error while attempting to manage nodes: #{ex.inspect}")
      reset
      sleep(TIMEOUT)
      retry
    rescue NoMasterError
      logger.error("Failed to promote a new master after #{MAX_PROMOTION_ATTEMPTS} attempts.")
      reset
      sleep(TIMEOUT)
      retry
    end

    # Notifies the manager of a state change. Used primarily by
    # {RedisFailover::NodeWatcher} to inform the manager of watched node states.
    #
    # @param [Node] node the node
    # @param [Symbol] state the state
    # @param [Integer] latency an optional latency
    def notify_state(node, state, latency = nil)
      @lock.synchronize do
        if running?
          update_current_state(node, state, latency)
        end
      end
    rescue => ex
      logger.error("Error handling state report #{[node, state].inspect}: #{ex.inspect}")
      logger.error(ex.backtrace.join("\n"))
    end

    # Performs a reset of the manager.
    def reset
      @master_manager = false
      @master_promotion_attempts = 0
      @watchers.each(&:shutdown) if @watchers
    end

    # Initiates a graceful shutdown.
    def shutdown
      logger.info('Shutting down ...')
      @lock.synchronize do
        @shutdown = true
      end

      reset
      exit
    end

    private

    # Configures the ZooKeeper client.
    def setup_zk
      unless @zk
        @zk = ZK.new("#{@options[:zkservers]}#{@options[:chroot] || ''}")
        @zk.register(manual_failover_path) do |event|
          handle_manual_failover_update(event)
        end
        @zk.on_connected { @zk.stat(manual_failover_path, :watch => true) }
      end

      create_path(@root_node)
      create_path(current_state_root)
      @zk.stat(manual_failover_path, :watch => true)
    end

    # Seeds the initial node master from an existing znode config.
    def find_existing_master
      if data = @zk.get(redis_nodes_path).first
        nodes = symbolize_keys(decode(data))
        master = node_from(nodes[:master])
        logger.info("Master from existing znode config: #{master || 'none'}")
        # Check for case where a node previously thought to be the master was somehow
        # manually reconfigured to be a slave outside of the node manager's control.
        begin
          if master && master.slave?
            raise InvalidNodeRoleError.new(master, :master, :slave)
          end
        rescue NodeUnavailableError => ex
          logger.warn("Failed to check whether existing znode master [#{master}] has invalid role: #{ex.inspect}")
        end

        master
      end
    rescue ZK::Exceptions::NoNode
      # blank slate, no last known master
      nil
    end

    # Spawns the {RedisFailover::NodeWatcher} instances for each managed node.
    def spawn_watchers
      @zk.delete(current_state_path, :ignore => :no_node)
      @monitored_available, @monitored_unavailable = {}, []
      @watchers = @nodes.map do |node|
        NodeWatcher.new(self, node, @options.fetch(:max_failures, 3))
      end
      @watchers.each(&:watch)
      logger.info("Monitoring redis nodes at #{stringify_nodes(@nodes)}")
    end

    # Deletes the znode path containing the redis nodes.
    #
    # @param [String] path the znode path to delete
    def delete_path(path)
      @zk.delete(path)
      logger.info("Deleted ZK node #{path}")
    rescue ZK::Exceptions::NoNode => ex
      logger.info("Tried to delete missing znode: #{ex.inspect}")
    end

    # Creates a znode path.
    #
    # @param [String] path the znode path to create
    # @param [Hash] options the options used to create the path
    # @option options [String] :initial_value an initial value for the znode
    # @option options [Boolean] :ephemeral true if node is ephemeral, false otherwise
    def create_path(path, options = {})
      unless @zk.exists?(path)
        @zk.create(path,
          options[:initial_value],
          :ephemeral => options.fetch(:ephemeral, false))
        logger.info("Created ZK node #{path}")
      end
    rescue ZK::Exceptions::NodeExists
      # best effort
    end

    # Writes state to a particular znode path.
    #
    # @param [String] path the znode path that should be written to
    # @param [String] value the value to write to the znode
    # @param [Hash] options the default options to be used when creating the node
    # @note the path will be created if it doesn't exist
    def write_state(path, value, options = {})
      create_path(path, options.merge(:initial_value => value))
      @zk.set(path, value)
    end

    # Handles a manual failover znode update.
    #
    # @param [ZK::Event] event the ZK event to handle
    def handle_manual_failover_update(event)
      if event.node_created? || event.node_changed?
        perform_manual_failover
      end
    rescue => ex
      logger.error("Error scheduling a manual failover: #{ex.inspect}")
      logger.error(ex.backtrace.join("\n"))
    ensure
      @zk.stat(manual_failover_path, :watch => true)
    end

    # Fetches each currently running node manager's view of the
    # world in terms of which nodes they think are available/unavailable.
    #
    # @return [Hash<String, Array>] a hash of node manager to host states
    def fetch_node_manager_states
      states = {}
      @zk.children(current_state_root).each do |child|
        full_path = "#{current_state_root}/#{child}"
        begin
          states[child] = symbolize_keys(decode(@zk.get(full_path).first))
        rescue ZK::Exceptions::NoNode
          # ignore, this is an edge case that can happen when a node manager
          # process dies while fetching its state
        rescue => ex
          logger.error("Failed to fetch states for #{full_path}: #{ex.inspect}")
        end
      end
      states
    end

    # Manages the redis nodes by periodically processing snapshots.
    def manage_nodes
      # Re-discover nodes, since the state of the world may have been changed
      # by the time we've become the primary node manager.
      discover_nodes

      # ensure that slaves are correctly pointing to this master
      redirect_slaves_to(@master)

      # Periodically update master config state.
      while running? && master_manager?
        @zk_lock.assert!
        sleep(CHECK_INTERVAL)

        @lock.synchronize do
          snapshots = current_node_snapshots
          if ensure_sufficient_node_managers(snapshots)

            sorted_snaps = snapshots.keys.sort_by {|node| node == @master ? 0 : 1 }  # process master node state first
            orig_master = @master

            sorted_snaps.each do |node|
              next if @master != orig_master && node == @master   # skip processing of the just-promoted slave in this cycle
              update_master_state(node, snapshots)
            end

            # flush current master state
            write_current_redis_nodes

            # check if we've exhausted our attempts to promote a master
            unless @master
              @master_promotion_attempts += 1
              raise NoMasterError if @master_promotion_attempts > MAX_PROMOTION_ATTEMPTS
            end
          end
        end
      end
    end

    # Executes a block wrapped in a ZK exclusive lock.
    def with_lock
      @zk_lock ||= @zk.locker(current_lock_path)

      begin
        @zk_lock.lock!(true)
      rescue Exception
        # handle shutdown case
        running? ? raise : return
      end

      if running?
        @zk_lock.assert!
        yield
      end
    ensure
      if @zk_lock
        begin
          @zk_lock.unlock!
        rescue => ex
          logger.warn("Failed to release lock: #{ex.inspect}")
        end
      end
    end

    # Perform a manual failover to a redis node.
    def perform_manual_failover
      @lock.synchronize do
        return unless running? && @master_manager && @zk_lock
        @zk_lock.assert!
        new_master = @zk.get(manual_failover_path, :watch => true).first
        return unless new_master && new_master.size > 0
        logger.info("Received manual failover request for: #{new_master}")
        logger.info("Current nodes: #{current_nodes.inspect}")
        snapshots = current_node_snapshots

        node = if new_master == ManualFailover::ANY_SLAVE
          failover_strategy_candidate(snapshots)
        else
          node_from(new_master)
        end

        if node
          handle_manual_failover(node, snapshots)
        else
          logger.error('Failed to perform manual failover, no candidate found.')
        end
      end
    rescue => ex
      logger.error("Error handling manual failover: #{ex.inspect}")
      logger.error(ex.backtrace.join("\n"))
    ensure
      @zk.stat(manual_failover_path, :watch => true)
    end
  end
end
