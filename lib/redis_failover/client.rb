module RedisFailover
  # Redis failover-aware client. RedisFailover::Client is a wrapper over a set of underlying redis
  # clients, which means all normal redis operations can be performed on an instance of this class.
  # The class only requires a set of ZooKeeper server addresses to function properly. The client
  # will automatically retry failed operations, and handle failover to a new master. The client
  # registers and listens for watcher events from the Node Manager. When these events are received,
  # the client fetches the latest set of redis nodes from ZooKeeper and rebuilds its internal
  # Redis clients appropriately. RedisFailover::Client also directs write operations to the master,
  # and all read operations to the slaves.
  #
  # Examples
  #
  #   client = RedisFailover::Client.new(:zkservers => 'localhost:2181,localhost:2182,localhost:2183')
  #   client.set('foo', 1) # will be directed to master
  #   client.get('foo') # will be directed to a slave
  #
  class Client
    include Util

    # Maximum allowed elapsed time between notifications from the Node Manager.
    # When this timeout is reached, the client will raise a NoNodeManagerError
    # and purge its internal redis clients.
    ZNODE_UPDATE_TIMEOUT = 9

    # Amount of time to sleep before retrying a failed operation.
    RETRY_WAIT_TIME = 3

    # Redis read operations that are automatically dispatched to slaves. Any
    # operation not listed here will be dispatched to the master.
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

    # Unsupported Redis operations. These don't make sense in a client
    # that abstracts the master/slave servers.
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
    #   :zkservers     - comma-separated ZooKeeper host:port pairs (required)
    #   :znode_path    - the Znode path override for redis server list (optional)
    #   :password      - password for redis nodes (optional)
    #   :db            - db to use for redis nodes (optional)
    #   :namespace     - namespace for redis nodes (optional)
    #   :logger        - logger override (optional)
    #   :retry_failure - indicate if failures should be retried (default true)
    #   :max_retries   - max retries for a failure (default 3)
    #
    def initialize(options = {})
      Util.logger = options[:logger] if options[:logger]
      @zkservers = options.fetch(:zkservers) { raise ArgumentError, ':zkservers required'}
      @znode = options[:znode_path] || Util::DEFAULT_ZNODE_PATH
      @namespace = options[:namespace]
      @password = options[:password]
      @db = options[:db]
      @retry = options[:retry_failure] || true
      @max_retries = @retry ? options.fetch(:max_retries, 3) : 0
      @master = nil
      @slaves = []
      @queue = Queue.new
      @lock = Monitor.new
      start_zk
      build_clients
    end

    # Dispatches redis operations to master/slaves.
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
      "#<RedisFailover::Client (master: #{master_name}, slaves: #{slave_names})>"
    end
    alias_method :to_s, :inspect

    def manual_failover(options = {})
      Manual.new(zk).failover(options)
      self
    end

    private

    def zk
      @lock.synchronize { @zk }
    end

    def start_zk
      @delivery_thread ||= Thread.new do
        while event = @queue.pop
          begin
            Proc === event ? event.call : handle_zk_event(event)
          rescue => ex
            logger.error("Error while handling event: #{ex.inspect}")
            logger.error(ex.backtrace.join("\n"))
          end
        end
      end

      reconnect_zk
    end

    def handle_session_established
      @lock.synchronize do
        @zk.watcher.register(@znode) do |event|
          @queue << event
        end
        @zk.on_expired_session do
          @queue << proc { reconnect_zk }
        end
        @zk.event_handler.register_state_handler(:connecting) do
          @queue << proc { handle_lost_connection }
        end
        @zk.on_connected do
          @zk.stat(@znode, :watch => true)
        end
        @zk.stat(@znode, :watch => true)
      end
    end

    def handle_zk_event(event)
      update_znode_timestamp
      if event.node_created? || event.node_changed?
        build_clients
      elsif event.node_deleted?
        purge_clients
        zk.stat(@znode, :watch => true)
      else
        logger.error("Unknown ZK node event: #{event.inspect}")
      end
    end

    def reconnect_zk
      @lock.synchronize do
        handle_lost_connection
        @zk.close! if @zk
        @zk = ZK.new(@zkservers)
        handle_session_established
        update_znode_timestamp
      end
    end

    def handle_lost_connection
      purge_clients
    end

    def redis_operation?(method)
      Redis.public_instance_methods(false).include?(method)
    end

    def dispatch(method, *args, &block)
      unless recently_heard_from_node_manager?
        @lock.synchronize do
          reconnect_zk
          build_clients
        end
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
        logger.error("Error while handling `#{method}` - #{ex.inspect}")
        logger.error(ex.backtrace.join("\n"))

        if tries < @max_retries
          tries += 1
          build_clients
          sleep(RETRY_WAIT_TIME)
          retry
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
        retried = false

        begin
          nodes = fetch_nodes
          return unless nodes_changed?(nodes)

          purge_clients
          logger.info("Building new clients for nodes #{nodes}")
          new_master = new_clients_for(nodes[:master]).first if nodes[:master]
          new_slaves = new_clients_for(*nodes[:slaves])
          @master = new_master
          @slaves = new_slaves
        rescue ZK::Exceptions::InterruptedSession => ex
          logger.error("ZK error while attempting to build clients: #{ex.inspect}")
          logger.error(ex.backtrace.join("\n"))

          # when ZK is disconnected, retry once
          unless retried
            reconnect_zk
            retried = true
            retry
          end
          raise
        end
      end
    end

    def fetch_nodes
      data = zk.get(@znode, :watch => true).first
      nodes = symbolize_keys(decode(data))
      logger.debug("Fetched nodes: #{nodes}")

      nodes
    end

    def new_clients_for(*nodes)
      nodes.map do |node|
        host, port = node.split(':')
        opts = {:host => host, :port => port}
        opts.update(:db => @db) if @db
        opts.update(:password => @password) if @password
        client = Redis.new(opts)
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
