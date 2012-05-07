module RedisFailover
  # Provides manual failover support to a new master.
  module Manual
    extend self

    # Path for manual failover communication.
    ZNODE_PATH = '/redis_failover_manual'.freeze

    # Denotes that any slave can be used as a candidate for promotion.
    ANY_SLAVE = "ANY_SLAVE".freeze

    # Performs a manual failover. If options is empty, a random slave will
    # be used as a failover candidate.
    #
    # @param [ZK] zk the ZooKeeper client
    # @param [Hash] options the options used for manual failover
    # @option options [String] :host the host of the failover candidate
    # @option options [String] :port the port of the failover candidate
    def failover(zk, options = {})
      create_path(zk)
      node = options.empty? ? ANY_SLAVE : "#{options[:host]}:#{options[:port]}"
      zk.set(ZNODE_PATH, node)
    end

    private

    # Creates the znode path used for coordinating manual failovers.
    #
    # @param [ZK] zk the ZooKeeper cilent
    def create_path(zk)
      zk.create(ZNODE_PATH)
    rescue ZK::Exceptions::NodeExists
      # best effort
    end
  end
end
