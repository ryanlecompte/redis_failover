module RedisFailover
  # NodeManager manages a list of redis nodes. Upon startup, the NodeManager
  # will discover the current redis master and slaves. Each redis node is
  # monitored by a NodeWatcher instance. The NodeWatchers periodically
  # report the current state of the redis node it's watching to the
  # NodeManager via an asynchronous queue. The NodeManager processes the
  # state reports and reacts appropriately by handling stale/dead nodes,
  # and promoting a new redis master if it sees fit to do so.
  class NodeManager
    include Util

    # Number of seconds to wait before retrying bootstrap process.
    TIMEOUT = 5

    # Creates a new instance.
    #
    # @param [Hash] options the options used to initialize the manager
    # @option options [String] :zkservers comma-separated ZK host:port pairs
    # @option options [String] :znode_path znode path override for redis nodes
    # @option options [String] :password password for redis nodes
    # @option options [Array<String>] :nodes the nodes to manage
    # @option options [String] :max_failures the max failures for a node
    def initialize(options)
      logger.info("Redis Node Manager v#{VERSION} starting (#{RUBY_DESCRIPTION})")
      @options = options
      @root_znode = options.fetch(:znode_path, Util::DEFAULT_ROOT_ZNODE_PATH)
      @manual_failover_mutex = Mutex.new
      @master_manager = false
    end

    # Starts the node manager.
    #
    # @note This method does not return until the manager terminates.
    def start
      @queue = Queue.new
      @master_manager = false
      setup_zk
      discover_nodes
      spawn_watchers
      handle_state_reports
    rescue ZK::Exceptions::InterruptedSession, ZKDisconnectedError => ex
      logger.error("ZK error while attempting to manage nodes: #{ex.inspect}")
      logger.error(ex.backtrace.join("\n"))
      shutdown
      sleep(TIMEOUT)
      retry
    end

    # Notifies the manager of a state change. Used primarily by
    # {RedisFailover::NodeWatcher} to inform the manager of watched node states.
    #
    # @param [Node] node the node
    # @param [Symbol] state the state
    def notify_state(node, state = nil)
      @queue << [node, state]
    end

    # Performs a graceful shutdown of the manager.
    def shutdown
      @queue.clear
      @queue << nil
      @watchers.each(&:shutdown) if @watchers
      @zk.close! if @zk
    end

    private

    # Configures the ZooKeeper client.
    def setup_zk
      @manager_lock.unlock if @manager_lock
      @zk.close! if @zk
      @zk = ZK.new("#{@options[:zkservers]}#{@options[:chroot] || ''}")
      create_path(@root_znode)
      create_path("#{@root_znode}/manager_node_state")

      @zk.on_expired_session { notify_state(:zk_disconnected) }
      @zk.register(manual_failover_path) do |event|
        @manual_failover_mutex.synchronize do
          if event.node_changed?
            schedule_manual_failover
          end
        end
      end

      @zk.on_connected { @zk.stat(manual_failover_path, :watch => true) }
      @zk.stat(manual_failover_path, :watch => true)
      @manager_lock = @zk.locker('master_node_manager_lock')
      @zk_lock_thread = Thread.new do
        wait_until_master_manager
      end
    end

    # Handles periodic state reports from {RedisFailover::NodeWatcher} instances.
    def handle_state_reports
      while state_report = @queue.pop
        begin
          node, state = state_report
          update_current_state(node, state)
          handle_master_state(node, state) if master_manager?
        rescue ZK::Exceptions::InterruptedSession, ZKDisconnectedError
          # fail hard if this is a ZK connection-related error
          raise
        rescue => ex
          logger.error("Error handling #{state_report.inspect}: #{ex.inspect}")
          logger.error(ex.backtrace.join("\n"))
        end
      end
    end

    # Handles an unavailable node.
    #
    # @param [Node] node the unavailable node
    def handle_unavailable(node)
      # no-op if we already know about this node
      return if @unavailable.include?(node)
      logger.info("Handling unavailable node: #{node}")

      @unavailable << node
      # find a new master if this node was a master
      if node == @master
        logger.info("Demoting currently unavailable master #{node}.")
        promote_new_master
      else
        @slaves.delete(node)
      end
    end

    # Handles an available node.
    #
    # @param [Node] node the available node
    def handle_available(node)
      reconcile(node)

      # no-op if we already know about this node
      return if @master == node || @slaves.include?(node)
      logger.info("Handling available node: #{node}")

      if @master
        # master already exists, make a slave
        node.make_slave!(@master)
        @slaves << node
      else
        # no master exists, make this the new master
        promote_new_master(node)
      end

      @unavailable.delete(node)
    end

    # Handles a node that is currently syncing.
    #
    # @param [Node] node the syncing node
    def handle_syncing(node)
      reconcile(node)

      if node.syncing_with_master? && node.prohibits_stale_reads?
        logger.info("Node #{node} not ready yet, still syncing with master.")
        force_unavailable_slave(node)
        return
      end

      # otherwise, we can use this node
      handle_available(node)
    end

    # Handles a manual failover request to the given node.
    #
    # @param [Node] node the candidate node for failover
    def handle_manual_failover(node)
      # no-op if node to be failed over is already master
      return if @master == node
      logger.info("Handling manual failover")

      # make current master a slave, and promote new master
      @slaves << @master
      @slaves.delete(node)
      promote_new_master(node)
    end

    # Promotes a new master.
    #
    # @param [Node] node the optional node to promote
    # @note if no node is specified, a random slave will be used
    def promote_new_master(node = nil)
      delete_path(redis_nodes_path)
      @master = nil

      # make a specific node or slave the new master
      candidate = node || @slaves.pop
      unless candidate
        logger.error('Failed to promote a new master, no candidate available.')
        return
      end

      redirect_slaves_to(candidate)
      candidate.make_master!
      @master = candidate

      write_current_redis_nodes
      logger.info("Successfully promoted #{candidate} to master.")
    end

    # Discovers the current master and slave nodes.
    def discover_nodes
      @reachable, @unreachable, @unavailable = [], [], []
      nodes = @options[:nodes].map { |opts| Node.new(opts) }.uniq
      @master = find_master(nodes)
      @slaves = nodes - [@master]
      logger.info("Monitoring master (#{@master}) and slaves" +
        " (#{@slaves.map(&:to_s).join(', ')})")
    end

    # Spawns the {RedisFailover::NodeWatcher} instances for each managed node.
    def spawn_watchers
      @watchers = [@master, @slaves, @unavailable].flatten.map do |node|
        NodeWatcher.new(self, node, @options[:max_failures] || 3)
      end
      @watchers.each(&:watch)
    end

    # Searches for the master node.
    #
    # @param [Array<Node>] nodes the nodes to search
    # @return [Node] the found master node, nil if not found
    def find_master(nodes)
      nodes.find do |node|
        begin
          node.master?
        rescue NodeUnavailableError
          false
        end
      end
    end

    # Redirects all slaves to the specified node.
    #
    # @param [Node] node the node to which slaves are redirected
    def redirect_slaves_to(node)
      @slaves.dup.each do |slave|
        begin
          slave.make_slave!(node)
        rescue NodeUnavailableError
          logger.info("Failed to redirect unreachable slave #{slave} to #{node}")
          force_unavailable_slave(slave)
        end
      end
    end

    # Forces a slave to be marked as unavailable.
    #
    # @param [Node] node the node to force as unavailable
    def force_unavailable_slave(node)
      @slaves.delete(node)
      @unavailable << node unless @unavailable.include?(node)
    end

    # It's possible that a newly available node may have been restarted
    # and completely lost its dynamically set run-time role by the node
    # manager. This method ensures that the node resumes its role as
    # determined by the manager.
    #
    # @param [Node] node the node to reconcile
    def reconcile(node)
      return if @master == node && node.master?
      return if @master && node.slave_of?(@master)

      logger.info("Reconciling node #{node}")
      if @master == node && !node.master?
        # we think the node is a master, but the node doesn't
        node.make_master!
        return
      end

      # verify that node is a slave for the current master
      if @master && !node.slave_of?(@master)
        node.make_slave!(@master)
      end
    end

    # @return [Hash] the set of current nodes grouped by category
    def current_nodes
      {
        :master => @master ? @master.to_s : nil,
        :slaves => @slaves.map(&:to_s),
        :unavailable => @unavailable.map(&:to_s)
      }
    end

    # @return [Hash] the set of currently reachable/unreachable nodes as
    # seen by this node manager instance
    def node_reachability_state
      {
        :reachable => @reachable.map(&:to_s),
        :unreachable => @unreachable.map(&:to_s)
      }
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

    # Schedules a manual failover to a redis node.
    def schedule_manual_failover
      return unless @master_manager
      new_master = @zk.get(manual_failover_path, :watch => true).first
      logger.info("Received manual failover request for: #{new_master}")

      node = if new_master == ManualFailover::ANY_SLAVE
        @slaves.sample
      else
        host, port = new_master.split(':', 2)
        Node.new(:host => host, :port => port, :password => @options[:password])
      end
      notify_state(node, :manual_failover) if node
    end

    # Produces a FQDN id for this Node Manager.
    #
    # @return [String] the FQDN for this Node Manager
    def manager_id
      @manager_id ||= [
        Socket.gethostbyname(Socket.gethostname)[0],
        Process.pid
      ].join('-')
    end

    # Writes the current master list of redis nodes. This method is only invoked
    # if this node manager instance is the master/primary manager.
    def write_current_redis_nodes
      write_state(redis_nodes_path, encode(current_nodes))
    end

    # @return [String] the znode path for this node manager's view
    # of reachable nodes
    def current_state_path
      [@root_znode, 'manager_node_state', manager_id].join('/')
    end

    # @return [String] the znode path for the master redis nodes config
    def redis_nodes_path
      "#{@root_znode}/nodes"
    end

    # Name for the znode that handles exclusive locking between multiple
    # Node Manager processes. Whoever holds the lock will be considered
    # the "master" Node Manager, and will be responsible for monitoring
    # the redis nodes. When a Node Manager that holds the lock disappears
    # or fails, another Node Manager process will grab the lock and
    # become the new master.
    def master_manager_lock_path
    end

    # @return [String] the znode path used for performing manual failovers
    def manual_failover_path
      ManualFailover.path(@root_znode)
    end

    # @return [Boolean] true if this node manager is the master, false otherwise
    def master_manager?
      @master_manager
    end

    # Used to update the master node manager state. These states are only handled if
    # this node manager instance is serving as the master manager.
    #
    # @param [Node] node the node to handle
    # @param [Symbol] state the node state
    def handle_master_state(node, state)
      case state
      when :unavailable
        handle_unavailable(node)
      when :available
        handle_available(node)
      when :syncing
        handle_syncing(node)
      when :manual_failover
        handle_manual_failover(node)
      when :zk_disconnected
        raise ZKDisconnectedError
      else
        raise InvalidNodeStateError.new(node, state)
      end

      # flush current master state
      write_current_redis_nodes
    end

    # Updates the current view of the world for this particular node
    # manager instance. All node managers write this state regardless
    # of whether they are the master manager or not.
    #
    # @param [Node] node the node to handle
    # @param [Symbol] state the node state
    def update_current_state(node, state)
      case state
      when :unavailable
        @unreachable << node unless @unreachable.include?(node)
        @reachable.delete(node)
      when :available, :syncing
        @reachable << node unless @reachable.include?(node)
        @unreachable.delete(node)
      when :zk_disconnected
        raise ZKDisconnectedError
      else
        raise InvalidNodeStateError.new(node, state)
      end

      # flush ephemeral current node manager state
      write_state(current_state_path,
        encode(node_reachability_state),
        :ephemeral => true)
    end

    # Waits until this node manager instance becomes the
    # master node manager that writes the master node config.
    def wait_until_master_manager
      logger.info('Waiting to become master Node Manager ...')
      @manager_lock.lock(true)
      @master_manager = true
      logger.info('Acquired master Node Manager lock')
    rescue => ex
      logger.error("Failed to become master Node Manager: #{ex.inspect}")
      logger.error(ex.backtrace.join("\n"))
      @master_manager = false
      notify_state(:zk_disconnected)
    end
  end
end
