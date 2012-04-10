module RedisFailover
  # NodeManager manages a list of redis nodes.
  class NodeManager
    include Util

    def initialize(nodes)
      @master, @slaves = parse_nodes(nodes)
      @unreachable = []
      @queue = Queue.new
      @lock = Mutex.new
    end

    def start
      spawn_watchers

      while node = @queue.pop
        begin
          if node.unreachable?
            handle_unreachable(node)
          elsif node.reachable?
            handle_reachable(node)
          end
        rescue NodeUnreachableError
          # node suddenly became unreachable, silently
          # handle since the watcher will take care of
          # keeping track of the node
        end
      end
    end

    def notify_state_change(node)
      @queue << node
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

    def handle_unreachable(node)
      @lock.synchronize do
        # no-op if we already know about this node
        return if @unreachable.include?(node)
        logger.info("Handling unreachable node: #{node}")
        @slaves.delete(node)

        # find a new master if this node was a master
        if node == @master
          logger.info("Demoting currently unreachable master #{node}.")
          promote_new_master
        end

        @unreachable << node
      end
    end

    def handle_reachable(node)
      @lock.synchronize do
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
      redirect_slaves!
      logger.info("Successfully promoted #{node} to master.")
    end

    def parse_nodes(nodes)
      nodes = nodes.map { |opts| Node.new(opts) }
      raise NoMasterError unless master = find_master(nodes)
      slaves = nodes - [master]

      logger.info("Managing master at #{master} and slaves" +
        " at #{slaves.map(&:to_s).join(', ')}")

      [master, slaves]
    end

    def spawn_watchers
      @watchers = [@master, *@slaves].map do |node|
          NodeWatcher.new(self, node)
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

    def redirect_slaves!
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
