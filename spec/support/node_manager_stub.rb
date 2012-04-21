module RedisFailover
  class NodeManagerStub < NodeManager
    attr_accessor :master
    public :current_nodes

    def initialize(options)
      super
      @zklock = Object.new
      @zklock.instance_eval do
        def with_lock
          yield
        end
      end
    end

    def discover_nodes
      # only discover nodes once in testing
      return if @nodes_discovered

      master = Node.new(:host => 'master')
      slave = Node.new(:host => 'slave')
      [master, slave].each { |node| node.extend(RedisStubSupport) }
      master.make_master!
      slave.make_slave!(master)
      @master = master
      @slaves = [slave]
      @nodes_discovered = true
    end

    def slaves
      @slaves
    end

    def start_processing
      @thread = Thread.new { start }
    end

    def stop_processing
      @queue << nil
      @thread.value
    end

    def force_unavailable(node)
      start_processing
      node.redis.make_unavailable!
      notify_state(node, :unavailable)
      stop_processing
    end

    def force_available(node)
      start_processing
      node.redis.make_available!
      notify_state(node, :available)
      stop_processing
    end

    def force_syncing(node, serve_stale_reads)
      start_processing
      node.redis.force_sync_with_master(serve_stale_reads)
      notify_state(node, :syncing)
      stop_processing
    end

    def initialize_path; end
    def delete_path; end
    def create_path; end
    def write_state; end
  end
end
