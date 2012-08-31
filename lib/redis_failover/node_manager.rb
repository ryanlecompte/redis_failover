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
    TIMEOUT = 3
    # ZK Errors that the Node Manager cares about.
    ZK_ERRORS = [
      ZK::Exceptions::LockAssertionFailedError,
      ZK::Exceptions::InterruptedSession,
      ZKDisconnectedError
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
      @znode = @options[:znode_path] || Util::DEFAULT_ZNODE_PATH
      @manual_znode = ManualFailover::ZNODE_PATH
      @mutex = Mutex.new
      @shutdown = false
      @leader = false
      @master = nil
      @slaves = []
      @unavailable = []
      @lock_path = "#{@znode}_lock".freeze
    end

    # Starts the node manager.
    #
    # @note This method does not return until the manager terminates.
    def start
      return unless running?
      @queue = Queue.new
      setup_zk
      logger.info('Waiting to become master Node Manager ...')
      with_lock do
        @leader = true
        logger.info('Acquired master Node Manager lock')
        discover_nodes
        initialize_path
        spawn_watchers
        handle_state_reports
      end
    rescue *ZK_ERRORS => ex
      logger.error("ZK error while attempting to manage nodes: #{ex.inspect}")
      reset
      retry
    end

    # Notifies the manager of a state change. Used primarily by
    # {RedisFailover::NodeWatcher} to inform the manager of watched node states.
    #
    # @param [Node] node the node
    # @param [Symbol] state the state
    def notify_state(node, state)
      @queue << [node, state]
    end

    # Performs a reset of the manager.
    def reset
      @leader = false
      @queue.clear
      @queue << nil
      @watchers.each(&:shutdown) if @watchers
      sleep(TIMEOUT)
      @zk.close! if @zk
      @zk_lock = nil
    end

    # Initiates a graceful shutdown.
    def shutdown
      @mutex.synchronize do
        @shutdown = true
        reset
      end
    end

    private

    # Configures the ZooKeeper client.
    def setup_zk
      @zk.close! if @zk
      @zk = ZK.new("#{@options[:zkservers]}#{@options[:chroot] || ''}")
      @zk.on_expired_session { notify_state(:zk_disconnected, nil) }

      @zk.register(@manual_znode) do |event|
        if event.node_created? || event.node_changed?
          perform_manual_failover
        end
      end

      @zk.on_connected { @zk.stat(@manual_znode, :watch => true) }
      @zk.stat(@manual_znode, :watch => true)
    end

    # Handles periodic state reports from {RedisFailover::NodeWatcher} instances.
    def handle_state_reports
      while running? && (state_report = @queue.pop)
        # Ensure that we still have the master lock.
        @zk_lock.assert!

        begin
          @mutex.synchronize do
            return unless running?
            node, state = state_report
            case state
            when :unavailable     then handle_unavailable(node)
            when :available       then handle_available(node)
            when :syncing         then handle_syncing(node)
            when :zk_disconnected then raise ZKDisconnectedError
            else raise InvalidNodeStateError.new(node, state)
            end

            # flush current state
            write_state
          end
        rescue *ZK_ERRORS
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
      delete_path
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

      create_path
      write_state
      logger.info("Successfully promoted #{candidate} to master.")
    end

    # Discovers the current master and slave nodes.
    def discover_nodes
      @mutex.synchronize do
        return unless running?
        nodes = @options[:nodes].map { |opts| Node.new(opts) }.uniq
        if @master = find_existing_master
          logger.info("Using master #{@master} from existing znode config.")
        elsif @master = guess_master(nodes)
          logger.info("Guessed master #{@master} from known redis nodes.")
        end
        @slaves = nodes - [@master]
        logger.info("Managing master (#{@master}) and slaves " +
          "(#{@slaves.map(&:to_s).join(', ')})")

        # ensure that slaves are correctly pointing to this master
        redirect_slaves_to(@master)
      end
    rescue NodeUnavailableError, NoMasterError, MultipleMastersError => ex
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
      if data = @zk.get(@znode).first
        nodes = symbolize_keys(decode(data))
        master = node_from(nodes[:master])
        logger.info("Master from existing znode config: #{master || 'none'}")
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
      @watchers = [@master, @slaves, @unavailable].flatten.compact.map do |node|
        NodeWatcher.new(self, node, @options[:max_failures] || 3)
      end
      @watchers.each(&:watch)
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

    # Deletes the znode path containing the redis nodes.
    def delete_path
      @zk.delete(@znode)
      logger.info("Deleted ZooKeeper node #{@znode}")
    rescue ZK::Exceptions::NoNode => ex
      logger.info("Tried to delete missing znode: #{ex.inspect}")
    end

    # Creates the znode path containing the redis nodes.
    def create_path
      unless @zk.exists?(@znode)
        @zk.create(@znode, encode(current_nodes))
        logger.info("Created ZooKeeper node #{@znode}")
      end
    rescue ZK::Exceptions::NodeExists
      # best effort
    end

    # Initializes the znode path containing the redis nodes.
    def initialize_path
      create_path
      write_state
    end

    # Writes the current redis nodes state to the znode path.
    def write_state
      create_path
      @zk.set(@znode, encode(current_nodes))
    end

    # Executes a block wrapped in a ZK exclusive lock.
    def with_lock
      @zk_lock = @zk.locker(@lock_path)
      @zk_lock.lock(true)
      yield
    ensure
      @zk_lock.unlock! if @zk_lock
    end

    # Perform a manual failover to a redis node.
    def perform_manual_failover
      @mutex.synchronize do
        return unless running? && @leader && @zk_lock
        @zk_lock.assert!
        new_master = @zk.get(@manual_znode, :watch => true).first
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
      @zk.stat(@manual_znode, :watch => true)
    end

    # @return [Boolean] true if running, false otherwise
    def running?
      !@shutdown
    end
  end
end
