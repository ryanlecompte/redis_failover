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

    # Name for the znode that handles exclusive locking between multiple
    # Node Manager processes. Whoever holds the lock will be considered
    # the "master" Node Manager, and will be responsible for monitoring
    # the redis nodes. When a Node Manager that holds the lock disappears
    # or fails, another Node Manager process will grab the lock and
    # become the master.
    LOCK_PATH = 'master_node_manager'

    # Number of seconds to wait before retrying bootstrap process.
    TIMEOUT = 3

    def initialize(options)
      logger.info("Redis Node Manager v#{VERSION} starting (#{RUBY_DESCRIPTION})")
      @options = options
      @znode = @options[:znode_path] || Util::DEFAULT_ZNODE_PATH
      @unavailable = []
      @queue = Queue.new
    end

    def start
      @zkclient = ZkClient.new(@options[:zkservers])
      logger.info('Waiting to become master Node Manager ...')
      @zkclient.with_lock(LOCK_PATH) do
        logger.info('Acquired master Node Manager lock')
        discover_nodes
        initialize_path
        spawn_watchers
        handle_state_reports
      end
    rescue *CONNECTIVITY_ERRORS => ex
      logger.error("Error while attempting to manage nodes: #{ex.inspect}")
      logger.error(ex.backtrace.join("\n"))
      sleep(TIMEOUT)
      retry
    end

    def notify_state(node, state)
      @queue << [node, state]
    end

    def shutdown
      @queue << nil
      @watchers.each(&:shutdown) if @watchers
    end

    private

    def handle_state_reports
      while state_report = @queue.pop
        begin
          node, state = state_report
          case state
          when :unavailable then handle_unavailable(node)
          when :available   then handle_available(node)
          when :syncing     then handle_syncing(node)
          else raise InvalidNodeStateError.new(node, state)
          end

          # flush current state
          write_state
        rescue StandardError, *CONNECTIVITY_ERRORS => ex
          logger.error("Error handling #{state_report.inspect}: #{ex.inspect}")
          logger.error(ex.backtrace.join("\n"))
        end
      end
    end

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

    def promote_new_master(node = nil)
      delete_path
      @master = nil

      # make a specific node or slave the new master
      candidate = node || @slaves.pop
      unless candidate
        logger.error('Failed to promote a new master since no candidate available.')
        return
      end

      redirect_slaves_to(candidate)
      candidate.make_master!
      @master = candidate

      create_path
      write_state
      logger.info("Successfully promoted #{candidate} to master.")
    end

    def discover_nodes
      nodes = @options[:nodes].map { |opts| Node.new(opts) }.uniq
      raise NoMasterError unless @master = find_master(nodes)
      @slaves = nodes - [@master]
      logger.info("Managing master (#{@master}) and slaves" +
        " (#{@slaves.map(&:to_s).join(', ')})")

      # ensure that slaves are correctly pointing to this master
      redirect_slaves_to(@master)
    end

    def spawn_watchers
      @watchers = [@master, @slaves, @unavailable].flatten.map do |node|
        NodeWatcher.new(self, node,  @options[:max_failures] || 3)
      end
      @watchers.each(&:watch)
    end

    def find_master(nodes)
      nodes.find do |node|
        begin
          node.master?
        rescue NodeUnavailableError
          false
        end
      end
    end

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

    def force_unavailable_slave(node)
      @slaves.delete(node)
      @unavailable << node unless @unavailable.include?(node)
    end

    # It's possible that a newly available node may have been restarted
    # and completely lost its dynamically set run-time role by the node
    # manager. This method ensures that the node resumes its role as
    # determined by the manager.
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

    def current_nodes
      {
        :master => @master ? @master.to_s : nil,
        :slaves => @slaves.map(&:to_s),
        :unavailable => @unavailable.map(&:to_s)
      }
    end

    def delete_path
      @zkclient.delete(@znode)
      logger.info("Deleted ZooKeeper node #{@znode}")
    rescue ZK::Exceptions::NoNode => ex
      logger.info("Tried to delete missing znode: #{ex.inspect}")
    end

    def create_path
      @zkclient.create(@znode, encode(current_nodes), :ephemeral => true)
      logger.info("Created ZooKeeper node #{@znode}")
    rescue ZK::Exceptions::NodeExists
      # best effort
    end

    def initialize_path
      create_path
      write_state
    end

    def write_state
      create_path
      @zkclient.set(@znode, encode(current_nodes))
    end
  end
end
