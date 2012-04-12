module RedisFailover
  # NodeManager manages a list of redis nodes.
  class NodeManager
    include Util

    def initialize(options)
      @options = options
      @max_failures = @options[:max_failures] || 3
      @master, @slaves = parse_nodes
      @unreachable = []
      @queue = Queue.new
      @lock = Mutex.new
    end

    def start
      spawn_watchers
      handle_state_changes
    end

    def notify_state_change(node, state)
      @queue << [node, state]
    end

    def nodes
      @lock.synchronize do
        {
          :master => @master ? @master.to_s : nil,
          :slaves => @slaves.map(&:to_s),
          :unreachable => @unreachable.map(&:to_s)
        }
      end
    end

    def shutdown
      @watchers.each(&:shutdown)
    end

    private

    def handle_state_changes
      while state_change = @queue.pop
        @lock.synchronize do
          node, state = state_change
          begin
            case state
            when :unreachable then handle_unreachable(node)
            when :reachable then handle_reachable(node)
            else raise InvalidNodeStateError.new(node, state)
            end
          rescue NodeUnreachableError
            # node suddenly became unreachable, silently
            # handle since the watcher will take care of
            # keeping track of the node
          end
        end
      end
    end

    def handle_unreachable(node)
      # no-op if we already know about this node
      return if @unreachable.include?(node)
      logger.info("Handling unreachable node: #{node}")

      # find a new master if this node was a master
      if node == @master
        logger.info("Demoting currently unreachable master #{node}.")
        promote_new_master
      else
        @slaves.delete(node)
      end

      @unreachable << node
    end

    def handle_reachable(node)
      # no-op if we already know about this node
      return if @master == node || @slaves.include?(node)
      logger.info("Handling reachable node: #{node}")

      @unreachable.delete(node)
      @slaves << node
      if @master
        # master already exists, make a slave
        node.make_slave!(@master)
      else
        # no master exists, make this the new master
        promote_new_master
      end
    end

    def promote_new_master
      @master = nil

      if @slaves.empty?
        logger.error('Failed to promote a new master since no slaves available.')
        return
      end

      # make a slave the new master
      node = @slaves.pop
      node.make_master!
      @master = node

      # switch existing slaves to point to new master
      redirect_slaves
      logger.info("Successfully promoted #{node} to master.")
    end

    def parse_nodes
      nodes = @options[:nodes].map { |opts| Node.new(opts) }
      raise NoMasterError unless master = find_master(nodes)
      slaves = nodes - [master]

      logger.info("Managing master (#{master}) and slaves" +
        " (#{slaves.map(&:to_s).join(', ')}), max failures #{@max_failures}")

      [master, slaves]
    end

    def spawn_watchers
      @watchers = [@master, *@slaves].map do |node|
          NodeWatcher.new(self, node, @max_failures)
      end
      @watchers.each(&:watch)
    end

    def find_master(nodes)
      # try to find the master - if the actual master is currently
      # down, it will be handled by its watcher
      nodes.find do |node|
        begin
          node.master?
        rescue NodeUnreachableError
          # will eventually be handled by watcher
          false
        end
      end
    end

    def redirect_slaves
      # redirect each slave to a new master - if an actual slave is
      # currently down, it will be handled by its watcher
      @slaves.each do |slave|
        begin
          slave.make_slave!(@master)
        rescue NodeUnreachableError
          # will eventually be handled by watcher
        end
      end
    end
  end
end
