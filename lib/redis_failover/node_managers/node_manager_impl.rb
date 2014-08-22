module RedisFailover
  # NodeManager manages a list of redis nodes. Upon startup, the NodeManager
  # will discover the current redis master and slaves. Each redis node is
  # monitored by a NodeWatcher instance. The NodeWatchers periodically
  # report the current state of the redis node it's watching to the
  # NodeManager. The NodeManager processes the state reports and reacts
  # appropriately by handling stale/dead nodes, and promoting a new redis master
  # if it sees fit to do so.
  class NodeManagerImpl
    include Util

    # Number of seconds to wait before retrying bootstrap process.
    TIMEOUT = 5
    # Number of seconds for checking node snapshots.
    CHECK_INTERVAL = 5
    # Number of max attempts to promote a master before releasing master lock.
    MAX_PROMOTION_ATTEMPTS = 3
    # Latency threshold for recording node state.
    LATENCY_THRESHOLD = 0.5

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
    # @option options [String] :node_path node path override for redis nodes
    # @option options [String] :password password for redis nodes
    # @option options [Array<String>] :nodes the nodes to manage
    # @option options [String] :max_failures the max failures for a node
    def initialize(options)
      logger.info("Redis Node Manager v#{VERSION} starting (#{RUBY_DESCRIPTION})")
      @options = options
      @required_node_managers = options.fetch(:required_node_managers, 1)
      @root_node = options[:node_path] || options[:znode_path] || Util::DEFAULT_ROOT_NODE_PATH
      @node_strategy = NodeStrategy.for(options.fetch(:node_strategy, :majority))
      @failover_strategy = FailoverStrategy.for(options.fetch(:failover_strategy, :latency))
      @nodes = Array(@options[:nodes]).map { |opts| Node.new(opts) }.uniq
      @master_manager = false
      @master_promotion_attempts = 0
      @sufficient_node_managers = false
      @lock = Monitor.new
      @shutdown = false
    end

    # Starts the node manager.
    #
    # @note This method does not return until the manager terminates.
    def start
      raise StandardError, 'Error: `start` needs to be implemented in child class.'
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

    # Handles an unavailable node.
    #
    # @param [Node] node the unavailable node
    # @param [Hash<Node, NodeSnapshot>] snapshots the current set of snapshots
    def handle_unavailable(node, snapshots)
      # no-op if we already know about this node
      return if @unavailable.include?(node)
      logger.info("Handling unavailable node: #{node}")

      @unavailable << node
      # find a new master if this node was a master
      if node == @master
        logger.info("Demoting currently unavailable master #{node}.")
        promote_new_master(snapshots)
      else
        @slaves.delete(node)
      end
    end

    # Handles an available node.
    #
    # @param [Node] node the available node
    # @param [Hash<Node, NodeSnapshot>] snapshots the current set of snapshots
    def handle_available(node, snapshots)
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
        promote_new_master(snapshots, node)
      end

      @unavailable.delete(node)
    end

    # Handles a node that is currently syncing.
    #
    # @param [Node] node the syncing node
    # @param [Hash<Node, NodeSnapshot>] snapshots the current set of snapshots
    def handle_syncing(node, snapshots)
      reconcile(node)

      if node.syncing_with_master? && node.prohibits_stale_reads?
        logger.info("Node #{node} not ready yet, still syncing with master.")
        force_unavailable_slave(node)
      else
        # otherwise, we can use this node
        handle_available(node, snapshots)
      end
    end

    # Handles a manual failover request to the given node.
    #
    # @param [Node] node the candidate node for failover
    # @param [Hash<Node, NodeSnapshot>] snapshots the current set of snapshots
    def handle_manual_failover(node, snapshots)
      # no-op if node to be failed over is already master
      return if @master == node
      logger.info("Handling manual failover")

      # ensure we can talk to the node
      node.ping

      # make current master a slave, and promote new master
      @slaves << @master if @master
      @slaves.delete(node)
      promote_new_master(snapshots, node)
    end

    # Promotes a new master.
    #
    # @param [Hash<Node, NodeSnapshot>] snapshots the current set of snapshots
    # @param [Node] node the optional node to promote
    def promote_new_master(snapshots, node = nil)
      delete_path(redis_nodes_path)
      @master = nil

      # make a specific node or selected candidate the new master
      candidate = node || failover_strategy_candidate(snapshots)

      if candidate.nil?
        # During master failure, 'unavailable' (for reads) slave nodes can actually still be perfectly electable (i.e. when slave-serve-stale-data is disabled)
        logger.info( "Unable to locate promotable slave from available snapshots: #{snapshots.inspect}" )
        logger.info( "Attempting to locate healthy slave via fallback discovery ..." )
        candidate = discover_electable_slave( @nodes )
      end

      if candidate.nil?
        logger.error('Failed to promote a new master, no candidate available.')
      else
        @slaves.delete(candidate)
        @unavailable.delete(candidate)
        redirect_slaves_to(candidate)
        candidate.make_master!
        @master = candidate
        write_current_redis_nodes
        @master_promotion_attempts = 0
        logger.info("Successfully promoted #{candidate} to master.")
      end
    end


    # Find the most master-electable (least-lagged) slave by querying all cluster nodes
    def discover_electable_slave( nodes )
      candidates = {}
      nodes.each do |node|
        score = node.electability rescue -1
        candidates[node] = score if score >= 0
      end
      logger.info("  Discovered electable slaves: #{candidates.inspect}")

      if candidate = candidates.min_by(&:last)
        candidate.first
      end
    end


    # Discovers the current master and slave nodes.
    # @return [Boolean] true if nodes successfully discovered, false otherwise
    def discover_nodes
      @lock.synchronize do
        return unless running?
        @slaves, @unavailable = [], []
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
      #logger.warn(ex.backtrace.join("\n"))
      sleep(TIMEOUT)
      retry
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
      "#{@root_node}/manager_node_state"
    end

    # @return [String] the znode path for this node manager's view
    # of available nodes
    def current_state_path
      "#{current_state_root}/#{manager_id}"
    end

    # @return [String] the znode path for the master redis nodes config
    def redis_nodes_path
      "#{@root_node}/nodes"
    end

    # @return [String] root path for current node manager lock
    def current_lock_path
      "#{@root_node}/master_redis_node_manager_lock"
    end

    # @return [String] the znode path used for performing manual failovers
    def manual_failover_path
      ManualFailover.path(@root_node)
    end

    # @return [Boolean] true if this node manager is the master, false otherwise
    def master_manager?
      @master_manager
    end

    # Used to update the master node manager state. These states are only handled if
    # this node manager instance is serving as the master manager.
    #
    # @param [Node] node the node to handle
    # @param [Hash<Node, NodeSnapshot>] snapshots the current set of snapshots
    def update_master_state(node, snapshots)
      state = @node_strategy.determine_state(node, snapshots)
      case state
      when :unavailable
        handle_unavailable(node, snapshots)
      when :available
        if node.syncing_with_master?
          handle_syncing(node, snapshots)
        else
          handle_available(node, snapshots)
        end
      else
        raise InvalidNodeStateError.new(node, state)
      end
    rescue *ZK_ERRORS, *ETCD_ERRORS
      # fail hard if this is a ZK/Etcd connection-related error
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
      old_unavailable = @monitored_unavailable.dup
      old_available = @monitored_available.dup

      case state
      when :unavailable
        unless @monitored_unavailable.include?(node)
          @monitored_unavailable << node
          @monitored_available.delete(node)
          write_current_monitored_state
        end
      when :available
        last_latency = @monitored_available[node]
        if last_latency.nil? || (latency - last_latency).abs > LATENCY_THRESHOLD
          @monitored_available[node] = latency
          @monitored_unavailable.delete(node)
          write_current_monitored_state
        end
      else
        raise InvalidNodeStateError.new(node, state)
      end
    rescue => ex
      # if an error occurs, make sure that we rollback to the old state
      @monitored_unavailable = old_unavailable
      @monitored_available = old_available
      raise
    end

    # Builds current snapshots of nodes across all running node managers.
    #
    # @return [Hash<Node, NodeSnapshot>] the snapshots for all nodes
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
        logger.info('Acquired master Node Manager lock.')
        logger.info("Configured node strategy #{@node_strategy.class}")
        logger.info("Configured failover strategy #{@failover_strategy.class}")
        logger.info("Required Node Managers to make a decision: #{@required_node_managers}")
        manage_nodes
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

    # @return [Boolean] true if running, false otherwise
    def running?
      @lock.synchronize { !@shutdown }
    end

    # @return [String] a stringified version of redis nodes
    def stringify_nodes(nodes)
      "(#{nodes.map(&:to_s).join(', ')})"
    end

    # Determines if each snapshot has a sufficient number of node managers.
    #
    # @param [Hash<Node, Snapshot>] snapshots the current snapshots
    # @return [Boolean] true if sufficient, false otherwise
    def ensure_sufficient_node_managers(snapshots)
      currently_sufficient = true
      available_managers = 0

      snapshots.each do |node, snapshot|
        node_managers = snapshot.node_managers
        available_managers = [available_managers, node_managers.size].max

        if node_managers.size < @required_node_managers
          logger.error("Not enough Node Managers in snapshot for node #{node}. " +
            "Required: #{@required_node_managers}, " +
            "Available: #{node_managers.size} #{node_managers}")
          currently_sufficient = false
        end
      end

      if currently_sufficient && !@sufficient_node_managers
        logger.info("Required Node Managers are visible: #{@required_node_managers}")
        if (available_managers - @required_node_managers) >= @required_node_managers
          logger.warn("WARNING: Required node managers (#{@required_node_managers}) less than majority available (#{available_managers}). You are vulnerable to network partition failures!")
        end
      end

      @sufficient_node_managers = currently_sufficient
      @sufficient_node_managers
    end

    # Invokes the configured failover strategy.
    #
    # @param [Hash<Node, NodeSnapshot>] snapshots the node snapshots
    # @return [Node] a failover candidate
    def failover_strategy_candidate(snapshots)
      # only include nodes that this master Node Manager can see
      filtered_snapshots = snapshots.select do |node, snapshot|
        snapshot.viewable_by?(manager_id)
      end

      logger.info('Attempting to find candidate from snapshots:')
      logger.info("\n" + filtered_snapshots.values.join("\n"))
      @failover_strategy.find_candidate(filtered_snapshots)
    end
  end
end
