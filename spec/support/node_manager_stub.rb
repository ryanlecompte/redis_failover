module RedisFailover
  class NodeManagerStub < ZookeeperNodeManager
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
      @failover_strategy = Object.new
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
      snapshot = OpenStruct.new(
        :node => node,
        :available_count => 0,
        :unavailable_count => 1,
        :node_managers => ['nm'])
      update_master_state(node, node => snapshot)
      stop_processing
    end

    def force_available(node)
      start_processing
      node.redis.make_available!
      snapshot = OpenStruct.new(
        :node => node,
        :available_count => 1,
        :unavailable_count => 0,
        :node_managers => ['nm'])
      update_master_state(node, node => snapshot)
      stop_processing
    end

    def force_syncing(node, serve_stale_reads)
      start_processing
      node.redis.force_sync_with_master(serve_stale_reads)
      snapshot = OpenStruct.new(
        :node => node,
        :available_count => 1,
        :unavailable_count => 0,
        :node_managers => ['nm'])
      update_master_state(node, node => snapshot)
      stop_processing
    end

    def failover_strategy_candidate(snapshots)
      @slaves.pop
    end

    def delete_path(*args); end
    def create_path(*args); end
    def write_state(*args); end
    def wait_until_master; end
    def current_node_snapshots; {} end
  end
end
