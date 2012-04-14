module RedisFailover
  class NodeManagerStub < NodeManager
    attr_accessor :master

    def parse_nodes
      master = Node.new(:host => 'master')
      slave = Node.new(:host => 'slave')
      [master, slave].each { |node| node.extend(RedisStubSupport) }
      master.make_master!
      slave.make_slave!(master)
      [master, [slave]]
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
      notify_state_change(node, :unavailable)
      stop_processing
    end

    def force_available(node)
      start_processing
      node.redis.make_available!
      notify_state_change(node, :available)
      stop_processing
    end

    def force_syncing(node, serve_stale_reads)
      start_processing
      node.redis.force_sync_with_master(serve_stale_reads)
      notify_state_change(node, :syncing)
      stop_processing
    end
  end
end
