module RedisFailover
  # Provides manual failover support to a new master.
  class ManualFailover
    # Path for manual failover communication.
    NODE_PATH = 'manual_failover'.freeze

    # Denotes that any slave can be used as a candidate for promotion.
    ANY_SLAVE = "ANY_SLAVE".freeze

    def self.path(root_node)
      "#{root_node}/#{NODE_PATH}"
    end

    # Creates a new instance.
    #
    # @param [StoreClient] config_client the node manager storage client (ZK or Etcd)
    # @param [Node] root_node the root ZK or Etcd node
    # @param [Hash] options the options used for manual failover
    # @option options [String] :host the host of the failover candidate
    # @option options [String] :port the port of the failover candidate
    # @note
    #   If options is empty, a random slave will be used
    #   as a failover candidate.
    def initialize(config_client, root_node, options = {})
      @config_client = config_client
      @root_node = root_node
      @options = options

      unless @options.empty?
        port = Integer(@options[:port]) rescue nil
        raise ArgumentError, ':host not properly specified' if @options[:host].to_s.empty?
        raise ArgumentError, ':port not properly specified' if port.nil?
      end
    end

    # Performs a manual failover.
    def perform
      create_path
      node = @options.empty? ? ANY_SLAVE : "#{@options[:host]}:#{@options[:port]}"
      client_set(node)
    end

    private

    # Creates the node path used for coordinating manual failovers.
    def create_path
      @config_client.create(self.class.path(@root_node))
    rescue ZK::Exceptions::NodeExists, *Util::ETCD_KEY_ERRORS
      # best effort
    end

    def client_set(value)
      if @config_client.is_a?(Etcd::Client)
        @config_client.set(self.class.path(@root_node), value: value)
      else
        @config_client.set(self.class.path(@root_node), value)
      end
    end
  end
end
