require 'set'

module RedisFailover
  # Redis failover-aware client.
  class Client
    include Util

    ZNODE_UPDATE_TIMEOUT = 9
    RETRY_WAIT_TIME = 3
    REDIS_READ_OPS = Set[
      :echo,
      :exists,
      :get,
      :getbit,
      :getrange,
      :hexists,
      :hget,
      :hgetall,
      :hkeys,
      :hlen,
      :hmget,
      :hvals,
      :keys,
      :lindex,
      :llen,
      :lrange,
      :mapped_hmget,
      :mapped_mget,
      :mget,
      :scard,
      :sdiff,
      :sinter,
      :sismember,
      :smembers,
      :srandmember,
      :strlen,
      :sunion,
      :type,
      :zcard,
      :zcount,
      :zrange,
      :zrangebyscore,
      :zrank,
      :zrevrange,
      :zrevrangebyscore,
      :zrevrank,
      :zscore
    ].freeze

    UNSUPPORTED_OPS = Set[
      :select,
      :ttl,
      :dbsize,
    ].freeze

    # Performance optimization: to avoid unnecessary method_missing calls,
    # we proactively define methods that dispatch to the underlying redis
    # calls.
    Redis.public_instance_methods(false).each do |method|
      define_method(method) do |*args, &block|
        dispatch(method, *args, &block)
      end
    end

    # Creates a new failover redis client.
    #
    # Options:
    #
    #   :zkservers - comma-separated ZooKeeper host:port pairs (required)
    #   :znode_path - the Znode path override for redis server list (optional)
    #   :password - password for redis nodes (optional)
    #   :namespace - namespace for redis nodes (optional)
    #   :logger - logger override (optional)
    #   :retry_failure - indicate if failures should be retried (default true)
    #   :max_retries - max retries for a failure (default 3)
    #
    def initialize(options = {})
      Util.logger = options[:logger] if options[:logger]
      @zkservers = options.fetch(:zkservers) { raise ArgumentError, ':zkservers required'}
      @znode = options[:znode_path] || Util::DEFAULT_ZNODE_PATH
      @namespace = options[:namespace]
      @password = options[:password]
      @retry = options[:retry_failure] || true
      @max_retries = @retry ? options.fetch(:max_retries, 3) : 1
      @master = nil
      @slaves = []
      @lock = Monitor.new
      setup_zookeeper_client
      build_clients
    end

    def method_missing(method, *args, &block)
      if redis_operation?(method)
        dispatch(method, *args, &block)
      else
        super
      end
    end

    def respond_to?(method)
      redis_operation?(method) || super
    end

    def inspect
      "#<RedisFailover::Client - master: #{master_name}, slaves: #{slave_names})>"
    end
    alias_method :to_s, :inspect

    private

    def setup_zookeeper_client
      @zkclient = ZkClient.new(@zkservers) do |client|
        # when session expires, purge client list
        client.on_session_expiration do
          purge_clients
        end

        # when we are disconnected, purge client list
        client.event_handler.register_state_handler(:connecting) do
          purge_clients
        end

        # when session is recovered, watch again
        client.on_session_recovered do
          client.stat(@znode, :watch => true)
        end

        # register a watcher for future changes
        client.watcher.register(@znode) do |event|
          if event.node_created? || event.node_changed?
            update_znode_timestamp
            build_clients
          elsif event.node_deleted?
            update_znode_timestamp
            purge_clients
            client.stat(@znode, :watch => true)
          else
            logger.error("Unknown ZK node event: #{event.inspect}")
          end
        end
      end
      update_znode_timestamp
    end

    def redis_operation?(method)
      Redis.public_instance_methods(false).include?(method)
    end

    def dispatch(method, *args, &block)
      unless recently_heard_from_node_manager?
        purge_clients
        raise MissingNodeManagerError.new(ZNODE_UPDATE_TIMEOUT)
      end

      verify_supported!(method)
      tries = 0
      begin
        if REDIS_READ_OPS.include?(method)
          # send read operations to a slave
          slave.send(method, *args, &block)
        else
          # direct everything else to master
          master.send(method, *args, &block)
        end
      rescue *CONNECTIVITY_ERRORS => ex
        logger.error("Error while handling operation `#{method}` - #{ex.inspect}")
        logger.error(ex.backtrace.join("\n"))

        if tries < @max_retries
          tries += 1
          build_clients
          sleep(RETRY_WAIT_TIME) && retry
        end

        raise
      end
    end

    def master
      if master = @lock.synchronize { @master }
        verify_role!(master, :master)
        return master
      end
      raise NoMasterError
    end

    def slave
      # pick a slave, if none available fallback to master
      if slave = @lock.synchronize { @slaves.sample }
        verify_role!(slave, :slave)
        return slave
      end
      master
    end

    def build_clients
      @lock.synchronize do
        tries = 0

        begin
          nodes = fetch_nodes
          return unless nodes_changed?(nodes)

          purge_clients
          logger.info("Building new clients for nodes #{nodes}")
          new_master = new_clients_for(nodes[:master]).first if nodes[:master]
          new_slaves = new_clients_for(*nodes[:slaves])
          @master = new_master
          @slaves = new_slaves
        rescue StandardError, *CONNECTIVITY_ERRORS => ex
          purge_clients
          logger.error("Failed to fetch nodes from #{@zkservers} - #{ex.inspect}")
          logger.error(ex.backtrace.join("\n"))

          if tries < @max_retries
            tries += 1
            sleep(RETRY_WAIT_TIME) && retry
          end

          raise
        end
      end
    end

    def fetch_nodes
      data = @zkclient.get(@znode, :watch => true).first
      nodes = symbolize_keys(decode(data))
      logger.debug("Fetched nodes: #{nodes}")

      nodes
    end

    def new_clients_for(*nodes)
      nodes.map do |node|
        host, port = node.split(':')
        client = Redis.new(:host => host, :port => port, :password => @password)
        if @namespace
          client = Redis::Namespace.new(@namespace, :redis => client)
        end
        client
      end
    end

    def master_name
      address_for(@master) || 'none'
    end

    def slave_names
      return 'none' if @slaves.empty?
      addresses_for(@slaves).join(', ')
    end

    def verify_role!(node, role)
      current_role = node.info['role']
      if current_role.to_sym != role
        raise InvalidNodeRoleError.new(address_for(node), role, current_role)
      end
      role
    end

    def verify_supported!(method)
      if UNSUPPORTED_OPS.include?(method)
        raise UnsupportedOperationError.new(method)
      end
    end

    def addresses_for(nodes)
      nodes.map { |node| address_for(node) }
    end

    def address_for(node)
      return unless node
      "#{node.client.host}:#{node.client.port}"
    end

    def nodes_changed?(new_nodes)
      return true if address_for(@master) != new_nodes[:master]
      return true if different?(addresses_for(@slaves), new_nodes[:slaves])
      false
    end

    def disconnect(*connections)
      connections.each do |conn|
        if conn
          begin
            conn.client.disconnect
          rescue
            # best effort
          end
        end
      end
    end

    def purge_clients
      @lock.synchronize do
        logger.info("Purging current redis clients")
        disconnect(@master, *@slaves)
        @master = nil
        @slaves = []
      end
    end

    def update_znode_timestamp
      @last_znode_timestamp = Time.now
    end

    def recently_heard_from_node_manager?
      return false unless @last_znode_timestamp
      Time.now - @last_znode_timestamp <= ZNODE_UPDATE_TIMEOUT
    end
  end
end
