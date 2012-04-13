module RedisFailover
  # NodeManager manages a list of redis nodes.
  class NodeManager
    include Util

    def initialize(options)
      @options = options
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

      @unreachable << node
      # find a new master if this node was a master
      if node == @master
        logger.info("Demoting currently unreachable master #{node}.")
        promote_new_master
      else
        @slaves.delete(node)
      end
    end

    def handle_reachable(node)
      # no-op if we already know about this node
      return if @master == node || @slaves.include?(node)
      logger.info("Handling reachable node: #{node}")

      if @master
        # master already exists, make a slave
        node.make_slave!(@master)
        @slaves << node
      else
        # no master exists, make this the new master
        promote_new_master(node)
      end

      @unreachable.delete(node)
    end

    def promote_new_master(node = nil)
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
      logger.info("Successfully promoted #{candidate} to master.")
    end

    def parse_nodes
      nodes = @options[:nodes].map { |opts| Node.new(opts) }
      raise NoMasterError unless master = find_master(nodes)
      slaves = nodes - [master]

      logger.info("Managing master (#{master}) and slaves" +
        " (#{slaves.map(&:to_s).join(', ')})")

      [master, slaves]
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
        rescue NodeUnreachableError
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
        rescue NodeUnreachableError
          # will also be detected by watcher
        end
      end
    end
  end
end
