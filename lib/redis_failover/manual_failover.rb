module RedisFailover
  # Provides manual failover support to a new master.
  class ManualFailover
    # Path for manual failover communication.
    ZNODE_PATH = '/redis_failover_manual'.freeze

    # Denotes that any slave can be used as a candidate for promotion.
    ANY_SLAVE = "ANY_SLAVE".freeze

    # Creates a new instance.
    #
    # @param [ZK] zk the ZooKeeper client
    # @param [Hash] options the options used for manual failover
    # @option options [String] :host the host of the failover candidate
    # @option options [String] :port the port of the failover candidate
    # @note
    #   If options is empty, a random slave will be used
    #   as a failover candidate.
    def initialize(zk, options = {})
      @zk = zk
      @options = options
    end

    # Performs a manual failover.
    def perform
      create_path
      node = @options.empty? ? ANY_SLAVE : "#{@options[:host]}:#{@options[:port]}"
      @zk.set(ZNODE_PATH, node)
    end

    private

    # Creates the znode path used for coordinating manual failovers.
    def create_path
      @zk.create(ZNODE_PATH)
    rescue ZK::Exceptions::NodeExists
      # best effort
    end
  end
end
