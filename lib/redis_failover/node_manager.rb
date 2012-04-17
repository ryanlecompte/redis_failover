module RedisFailover
  # NodeManager manages a list of redis nodes.
  class NodeManager
    include Util

    def initialize(options)
      @options = options
      @zkclient = ZkClient.new(@options[:zkservers])
      @znode = @options[:znode_path] || Util::DEFAULT_ZNODE_PATH
      @unavailable = []
      @queue = Queue.new
      discover_nodes
    end

    def start
      initialize_path
      spawn_watchers
      handle_state_changes
    end

    def notify_state_change(node, state)
      @queue << [node, state]
    end

    def shutdown
      @watchers.each(&:shutdown)
    end

    private

    def handle_state_changes
      while state_change = @queue.pop
        begin
          node, state = state_change
          case state
          when :unavailable then handle_unavailable(node)
          when :available   then handle_available(node)
          when :syncing     then handle_syncing(node)
          else raise InvalidNodeStateError.new(node, state)
          end

          # flush current state
          write_state
        rescue *ALL_ERRORS => ex
          logger.error("Error while handling #{state_change.inspect}: #{ex.message}")
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

      if node.prohibits_stale_reads?
        logger.info("Node #{node} not ready yet, still syncing with master.")
        @unavailable << node
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

      candidate.make_master!
      @master = candidate
      redirect_slaves_to_master
      create_path
      write_state
      logger.info("Successfully promoted #{candidate} to master.")
    end

    def discover_nodes
      nodes = @options[:nodes].map { |opts| Node.new(opts) }.uniq
      raise NoMasterError unless @master = find_master(nodes)
      @slaves = nodes - [@master]

      # ensure that slaves are correctly pointing to this master
      redirect_slaves_to_master

      logger.info("Managing master (#{@master}) and slaves" +
        " (#{@slaves.map(&:to_s).join(', ')})")
    end

    def spawn_watchers
      @watchers = [@master, *@slaves].map do |node|
        NodeWatcher.new(self, node,  @options[:max_failures] || 3)
      end
      @watchers.each(&:watch)
    end

    def find_master(nodes)
      # try to find the master - if the actual master is currently
      # down, it will be handled by its watcher
      nodes.find do |node|
        begin
          node.master?
        rescue NodeUnavailableError
          # will eventually be handled by watcher
          false
        end
      end
    end

    def redirect_slaves_to_master
      # redirect each slave to the current master
      @slaves.each do |slave|
        begin
          slave.make_slave!(@master)
        rescue NodeUnavailableError
          # will also be detected by watcher
        end
      end
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
      logger.info("Deleted zookeeper node #{@znode}")
    rescue ZK::Exceptions::NoNode => ex
      logger.info("Tried to delete missing znode: #{ex.message}")
    end

    def create_path
      @zkclient.create(@znode, encode(current_nodes), :ephemeral => true)
      logger.info("Created zookeeper node #{@znode}")
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
