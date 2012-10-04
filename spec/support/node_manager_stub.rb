module RedisFailover
  class NodeManagerStub < NodeManager
    attr_accessor :master
    # HACK - this will go away once we refactor the tests to use a real ZK/Redis server.
    public :current_nodes, :guess_master

    def discover_nodes
      # only discover nodes once in testing
      return true if @nodes_discovered

      master = Node.new(:host => 'master')
      slave = Node.new(:host => 'slave')
      [master, slave].each { |node| node.extend(RedisStubSupport) }
      master.make_master!
      slave.make_slave!(master)
      @nodes = [master, slave]
      @unavailable = []
      @master = master
      @slaves = [slave]
      @nodes_discovered = true
    end

    def setup_zk
      @zk = NullObject.new
    end

    def slaves
      @slaves
    end

    def start_processing
      @thread = Thread.new { start }
      sleep(1.5)
    end

    def stop_processing
      @thread.value
    end

    def force_unavailable(node)
      start_processing
      node.redis.make_unavailable!
      snapshot = OpenStruct.new(:available_count => 0, :unavailable_count => 1)
      update_master_state(node, snapshot)
      stop_processing
    end

    def force_available(node)
      start_processing
      node.redis.make_available!
      snapshot = OpenStruct.new(:available_count => 1, :unavailable_count => 0)
      update_master_state(node, snapshot)
      stop_processing
    end

    def force_syncing(node, serve_stale_reads)
      start_processing
      node.redis.force_sync_with_master(serve_stale_reads)
      snapshot = OpenStruct.new(:available_count => 1, :unavailable_count => 0)
      update_master_state(node, snapshot)
      stop_processing
    end

    def delete_path(*args); end
    def create_path(*args); end
    def write_state(*args); end
    def wait_until_master; end
  end
end
