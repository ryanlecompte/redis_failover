module RedisFailover
  # Provides manual failover support to a new master.
  class Manual
    include Util

    # Path for manual failover communication.
    ZNODE_PATH = '/redis_failover_manual'.freeze

    # Denotes that any slave can be used as a candidate for promotion.
    ANY_SLAVE = "ANY_SLAVE".freeze

    def initialize(zk)
      @zk = zk
    end

    # Manual failover to a new server. A specific server can be specified via:
    #   :host => host, :port => port
    def failover(options = {})
      create_path
      node = options.empty? ? ANY_SLAVE : "#{options[:host]}:#{options[:port]}"
      @zk.set(ZNODE_PATH, node)
    end

    private

    def create_path
      @zk.create(ZNODE_PATH)
      logger.info("Created ZooKeeper node #{ZNODE_PATH}")
    rescue ZK::Exceptions::NodeExists
      # best effort
    end
  end
end
