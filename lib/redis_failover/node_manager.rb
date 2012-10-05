module RedisFailover
  # NodeManager manages a list of redis nodes. Upon startup, the NodeManager
  # will discover the current redis master and slaves. Each redis node is
  # monitored by a NodeWatcher instance. The NodeWatchers periodically
  # report the current state of the redis node it's watching to the
  # NodeManager. The NodeManager processes the state reports and reacts
  # appropriately by handling stale/dead nodes, and promoting a new redis master
  # if it sees fit to do so.
  class NodeManager
    include Util

    # Number of seconds to wait before retrying bootstrap process.
    TIMEOUT = 5
    # Number of seconds for checking node snapshots.
    CHECK_INTERVAL = 10

    # ZK Errors that the Node Manager cares about.
    ZK_ERRORS = [
      ZK::Exceptions::LockAssertionFailedError,
      ZK::Exceptions::InterruptedSession
    ].freeze

    # Errors that can happen during the node discovery process.
    NODE_DISCOVERY_ERRORS = [
      InvalidNodeRoleError,
      NodeUnavailableError,
      NoMasterError,
      MultipleMastersError
    ].freeze

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
      @node_strategy = NodeStrategy.for(options.fetch(:node_strategy, :majority))
      @nodes = Array(@options[:nodes]).map { |opts| Node.new(opts) }.uniq
      @master_manager = false
      @lock = Mutex.new
      @shutdown = false
    end

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
    end

    # Notifies the manager of a state change. Used primarily by
    # {RedisFailover::NodeWatcher} to inform the manager of watched node states.
    #
    # @param [Node] node the node
    # @param [Symbol] state the state
    # @param [Integer] latency an optional latency
    def notify_state(node, state, latency = nil)
      @lock.synchronize do
        update_current_state(node, state, latency)
      end
    rescue => ex
      logger.error("Error handling state report #{[node, state].inspect}: #{ex.inspect}")
      logger.error(ex.backtrace.join("\n"))
    end

    # Performs a reset of the manager.
    def reset
      @master_manager = false
      @watchers.each(&:shutdown) if @watchers
      @zk.close! if @zk
      @zk_lock = nil
    end

    # Initiates a graceful shutdown.
    def shutdown
      logger.info('Shutting down ...')
      @lock.synchronize do
        @shutdown = true
        exit
      end
    end

    private

    # Configures the ZooKeeper client.
    def setup_zk
      @zk.close! if @zk
      @zk = ZK.new("#{@options[:zkservers]}#{@options[:chroot] || ''}")
      create_path(@root_znode)
      create_path(current_state_root)

      @zk.register(manual_failover_path) do |event|
        if event.node_created? || event.node_changed?
          perform_manual_failover
        end
      end

      @zk.on_connected { @zk.stat(manual_failover_path, :watch => true) }
      @zk.stat(manual_failover_path, :watch => true)
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
      return if @master == node || (@master && @slaves.include?(node))
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
      @slaves << @master if @master
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
    # @return [Boolean] true if nodes successfully discovered, false otherwise
    def discover_nodes
      @lock.synchronize do
        return unless running?
        @unavailable = []
        if @master = find_existing_master
          logger.info("Using master #{@master} from existing znode config.")
        elsif @master = guess_master(@nodes)
          logger.info("Guessed master #{@master} from known redis nodes.")
        end
        @slaves = @nodes - [@master]
        logger.info("Managing master (#{@master}) and slaves #{stringify_nodes(@slaves)}")
      end
    rescue *NODE_DISCOVERY_ERRORS => ex
      msg = <<-MSG.gsub(/\s+/, ' ')
        Failed to discover master node: #{ex.inspect}
        In order to ensure a safe startup, redis_failover requires that all redis
        nodes be accessible, and only a single node indicating that it's the master.
        In order to fix this, you can perform a manual failover via redis_failover,
        or manually fix the individual redis servers. This discovery process will
        retry in #{TIMEOUT}s.
      MSG
      logger.warn(msg)
      sleep(TIMEOUT)
      retry
    end

    # Seeds the initial node master from an existing znode config.
    def find_existing_master
      if data = @zk.get(redis_nodes_path).first
        nodes = symbolize_keys(decode(data))
        master = node_from(nodes[:master])
        logger.info("Master from existing znode config: #{master || 'none'}")
        # Check for case where a node previously thought to be the master was
        # somehow manually reconfigured to be a slave outside of the node manager's
        # control.
        if master && master.slave?
          raise InvalidNodeRoleError.new(master, :master, :slave)
        end
        master
      end
    rescue ZK::Exceptions::NoNode
      # blank slate, no last known master
      nil
    end

    # Creates a Node instance from a string.
    #
    # @param [String] node_string a string representation of a node (e.g., host:port)
    # @return [Node] the Node representation
    def node_from(node_string)
      return if node_string.nil?
      host, port = node_string.split(':', 2)
      Node.new(:host => host, :port => port, :password => @options[:password])
    end

    # Spawns the {RedisFailover::NodeWatcher} instances for each managed node.
    def spawn_watchers
      @monitored_available, @monitored_unavailable = {}, []
      @watchers = @nodes.map do |node|
        NodeWatcher.new(self, node, @options.fetch(:max_failures, 3))
      end
      @watchers.each(&:watch)
      logger.info("Monitoring redis nodes at #{stringify_nodes(@nodes)}")
    end

    # Searches for the master node.
    #
    # @param [Array<Node>] nodes the nodes to search
    # @return [Node] the found master node, nil if not found
    def guess_master(nodes)
      master_nodes = nodes.select { |node| node.master? }
      raise NoMasterError if master_nodes.empty?
      raise MultipleMastersError.new(master_nodes) if master_nodes.size > 1
      master_nodes.first
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

    # @return [Hash] the set of currently available/unavailable nodes as
    # seen by this node manager instance
    def node_availability_state
      {
        :available => Hash[@monitored_available.map { |k, v| [k.to_s, v] }],
        :unavailable => @monitored_unavailable.map(&:to_s)
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

    # Handles a manual failover znode update.
    #
    # @param [ZK::Event] event the ZK event to handle
    def handle_manual_failover_update(event)
      if event.node_created? || event.node_changed?
        @lock.synchronize do
          perform_manual_failover
        end
      end
    rescue => ex
      logger.error("Error scheduling a manual failover: #{ex.inspect}")
      logger.error(ex.backtrace.join("\n"))
    ensure
      @zk.stat(manual_failover_path, :watch => true)
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

    # @return [String] root path for current node manager state
    def current_state_root
      "#{@root_znode}/manager_node_state"
    end

    # @return [String] the znode path for this node manager's view
    # of available nodes
    def current_state_path
      "#{current_state_root}/#{manager_id}"
    end

    # @return [String] the znode path for the master redis nodes config
    def redis_nodes_path
      "#{@root_znode}/nodes"
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
    # @param [NodeSnapshot] snapshot the node snapshot
    def update_master_state(node, snapshot)
      state = @node_strategy.determine_state(snapshot)
      case state
      when :unavailable
        logger.info(snapshot)
        handle_unavailable(node)
      when :available
        if node.syncing_with_master?
          handle_syncing(node)
        else
          handle_available(node)
        end
      else
        raise InvalidNodeStateError.new(node, state)
      end
    rescue *ZK_ERRORS
      # fail hard if this is a ZK connection-related error
      raise
    rescue => ex
      logger.error("Error handling state report for #{[node, state].inspect}: #{ex.inspect}")
    end

    # Updates the current view of the world for this particular node
    # manager instance. All node managers write this state regardless
    # of whether they are the master manager or not.
    #
    # @param [Node] node the node to handle
    # @param [Symbol] state the node state
    # @param [Integer] latency an optional latency
    def update_current_state(node, state, latency = nil)
      case state
      when :unavailable
        @monitored_unavailable |= [node]
        @monitored_available.delete(node)
      when :available
        @monitored_available[node] = latency
        @monitored_unavailable.delete(node)
      else
        raise InvalidNodeStateError.new(node, state)
      end

      # flush ephemeral current node manager state
      write_state(current_state_path,
        encode(node_availability_state),
        :ephemeral => true)
    end

    # Fetches each currently running node manager's view of the
    # world in terms of which nodes they think are available/unavailable.
    #
    # @return [Hash<String, Array>] a hash of node manager to host states
    def fetch_node_manager_states
      states = {}
      @zk.children(current_state_root).each do |child|
        full_path = "#{current_state_root}/#{child}"
        states[child] = symbolize_keys(decode(@zk.get(full_path).first))
      end
      states
    end

    # Builds current snapshots of nodes across all running node managers.
    #
    # @return [Hash<String, NodeSnapshot>] the snapshots for all nodes
    def current_node_snapshots
      nodes = {}
      snapshots = Hash.new { |h, k| h[k] = NodeSnapshot.new(k) }
      fetch_node_manager_states.each do |node_manager, states|
        available, unavailable = states.values_at(:available, :unavailable)
        available.each do |node_string, latency|
          node = nodes[node_string] ||= node_from(node_string)
          snapshots[node].viewable_by(node_manager, latency)
        end
        unavailable.each do |node_string|
          node = nodes[node_string] ||= node_from(node_string)
          snapshots[node].unviewable_by(node_manager)
        end
      end

      snapshots
    end

    # Waits until this node manager becomes the master.
    def wait_until_master
      logger.info('Waiting to become master Node Manager ...')

      with_lock do
        @master_manager = true
        logger.info("Acquired master Node Manager lock. Using strategy #{strategy_name}")
        # Re-discover nodes, since the state of the world may have been changed
        # by the time we've become the primary node manager.
        discover_nodes

        # ensure that slaves are correctly pointing to this master
        redirect_slaves_to(@master)

        # Periodically update master config state.
        while running? && master_manager?
          @zk_lock.assert!
          @lock.synchronize do
            current_node_snapshots.each do |node, snapshot|
              update_master_state(node, snapshot)
            end

            # flush current master state
            write_current_redis_nodes
          end

          sleep(CHECK_INTERVAL)
        end
      end
    end

    # Creates a Node instance from a string.
    #
    # @param [String] node_string a string representation of a node (e.g., host:port)
    # @return [Node] the Node representation
    def node_from(node_string)
      return if node_string.nil?
      host, port = node_string.split(':', 2)
      Node.new(:host => host, :port => port, :password => @options[:password])
    end

    # Executes a block wrapped in a ZK exclusive lock.
    def with_lock
      @zk_lock = @zk.locker('master_redis_node_manager_lock')

      begin
        @zk_lock.lock!(true)
      rescue Exception
        # handle shutdown case
        running? ? raise : return
      end

      if running?
        yield
      end
    ensure
      @zk_lock.unlock! if @zk_lock
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
        node = new_master == ManualFailover::ANY_SLAVE ?
          @slaves.shuffle.first : node_from(new_master)
        if node
          handle_manual_failover(node)
        else
          logger.error('Failed to perform manual failover, no candidate found.')
        end
      end
    rescue => ex
      logger.error("Error handling a manual failover: #{ex.inspect}")
      logger.error(ex.backtrace.join("\n"))
    ensure
      @zk.stat(manual_failover_path, :watch => true)
    end

    # @return [Boolean] true if running, false otherwise
    def running?
      !@shutdown
    end

    # @return [String] the name of the node strategy
    def strategy_name
      @node_strategy.class.to_s.split('::').last.upcase
    end

    # @return [String] a stringified version of redis nodes
    def stringify_nodes(nodes)
      "(#{nodes.map(&:to_s).join(', ')})"
    end
  end
end
