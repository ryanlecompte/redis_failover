module RedisFailover
  class NodeManagerStub < NodeManager
    def parse_nodes(*)
      master = Node.new(:host => 'master')
      slave = Node.new(:host => 'slave')
      [master, slave].each { |node| node.extend(RedisStubSupport) }
      master.make_master!
      slave.make_slave!(master)
      [master, [slave]]
    end

    def master
      @master
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

    def force_unreachable(node)
      start_processing
      node.redis.make_unreachable!
      notify_state_change(node, :unreachable)
      stop_processing
    end

    def force_reachable(node)
      start_processing
      node.redis.make_reachable!
      notify_state_change(node, :reachable)
      stop_processing
    end
  end
end
