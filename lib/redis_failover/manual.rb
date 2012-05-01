module RedisFailover
  # Provides manual failover support to a new master.
  module Manual
    extend self

    # Path for manual failover communication.
    ZNODE_PATH = '/redis_failover_manual'.freeze

    # Denotes that any slave can be used as a candidate for promotion.
    ANY_SLAVE = "ANY_SLAVE".freeze

    def failover(zk, options = {})
      create_path(zk)
      node = options.empty? ? ANY_SLAVE : "#{options[:host]}:#{options[:port]}"
      zk.set(ZNODE_PATH, node)
    end

    private

    def create_path(zk)
      zk.create(ZNODE_PATH)
    rescue ZK::Exceptions::NodeExists
      # best effort
    end
  end
end
