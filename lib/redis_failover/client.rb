module RedisFailover
  # Redis failover-aware client. RedisFailover::Client is a wrapper over a set
  # of underlying redis clients, which means all normal redis operations can be
  # performed on an instance of this class. The class only requires a set of
  # ZooKeeper server addresses to function properly. The client will automatically
  # retry failed operations, and handle failover to a new master. The client
  # registers and listens for watcher events from the Node Manager. When these
  # events are received, the client fetches the latest set of redis nodes from
  # ZooKeeper and rebuilds its internal Redis clients appropriately.
  # RedisFailover::Client also directs write operations to the master, and all
  # read operations to the slaves.
  #
  # @example Usage
  #   zk_servers = 'localhost:2181,localhost:2182,localhost:2183'
  #   client = RedisFailover::Client.new(:zkservers => zk_servers)
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

    # Performance optimization: to avoid unnecessary method_missing calls,
    # we proactively define methods that dispatch to the underlying redis
    # calls.
    Redis.public_instance_methods(false).each do |method|
      define_method(method) do |*args, &block|
        dispatch(method, *args, &block)
      end
    end

    def call(command, &block)
      method = command[0]
      args = command[1..-1]
      dispatch(method, *args, &block)
    end

    # Creates a new failover redis client.
    #
    # @param [Hash] options the options used to initialize the client instance
    # @option options [String] :zkservers comma-separated ZooKeeper host:port
    # @option options [String] :zk an existing ZK client connection instance
    # @option options [String] :znode_path znode path override for redis nodes
    # @option options [String] :password password for redis nodes
    # @option options [String] :db database to use for redis nodes
    # @option options [String] :namespace namespace for redis nodes
    # @option options [Logger] :logger logger override
    # @option options [Boolean] :retry_failure indicates if failures are retried
    # @option options [Integer] :max_retries max retries for a failure
    # @option options [Boolean] :safe_mode indicates if safe mode is used or not
    # @option options [Boolean] :master_only indicates if only redis master is used
    # @option options [Boolean] :verify_role verify the actual role of a redis node before every command
    # @note Use either :zkservers or :zk
    # @return [RedisFailover::Client]
    def initialize(options = {})
      Util.logger = options[:logger] if options[:logger]
      @master = nil
      @slaves = []
      @node_addresses = {}
      @lock = Monitor.new
      @current_client_key = "current-client-#{self.object_id}"
      yield self if block_given?

      parse_options(options)
      setup_zk
      build_clients
    end

    # Stubs this method to return this RedisFailover::Client object.
    #
    # Some libraries (Resque) assume they can access the `client` via this method,
    # but we don't want to actually ever expose the internal Redis connections.
    #
    # By returning `self` here, we can add stubs for functionality like #reconnect,
    # and everything will Just Work.
    #
    # Takes an *args array for safety only.
    #
    # @return [RedisFailover::Client]
    def client(*args)
      self
    end

    # Delegates to the underlying Redis client to fetch the location.
    # This method always returns the location of the master.
    #
    # @return [String] the redis location
    def location
      dispatch(:client).location
    end

    # Specifies a callback to invoke when the current redis node list changes.
    #
    # @param [Proc] a callback with current master and slaves as arguments
    #
    # @example Usage
    #   RedisFailover::Client.new(:zkservers => zk_servers) do |client|
    #     client.on_node_change do |master, slaves|
    #       logger.info("Nodes changed! master: #{master}, slaves: #{slaves}")
    #     end
    #   end
    def on_node_change(&callback)
      @on_node_change = callback
    end

    # Dispatches redis operations to master/slaves.
    def method_missing(method, *args, &block)
      if redis_operation?(method)
        dispatch(method, *args, &block)
      else
        super
      end
    end

    # Determines whether or not an unknown method can be handled.
    #
    # @param [Symbol] method the method to check
    # @param [Boolean] include_private determines if private methods are checked
    # @return [Boolean] indicates if the method can be handled
    def respond_to_missing?(method, include_private)
      redis_operation?(method) || super
    end

    # @return [String] a string representation of the client
    def inspect
      "#<RedisFailover::Client (db: #{@db.to_i}, master: #{master_name}, slaves: #{slave_names})>"
    end
    alias_method :to_s, :inspect

    # Force a manual failover to a new server. A specific server can be specified
    # via options. If no options are passed, a random slave will be selected as
    # the candidate for the new master.
    #
    # @param [Hash] options the options used for manual failover
    # @option options [String] :host the host of the failover candidate
    # @option options [String] :port the port of the failover candidate
    def manual_failover(options = {})
      ManualFailover.new(@zk, @root_znode, options).perform
      self
    end

    # Gracefully performs a shutdown of this client. This method is
    # mostly useful when the client is used in a forking environment.
    # When a fork occurs, you can call this method in an after_fork hook,
    # and then create a new instance of the client. The underlying
    # ZooKeeper client and redis clients will be closed.
    def shutdown
      @zk.close! if @zk
      @zk = nil
      purge_clients
    end

    # Reconnect will first perform a shutdown of the underlying redis clients.
    # Next, it attempts to reopen the ZooKeeper client and re-create the redis
    # clients after it fetches the most up-to-date list from ZooKeeper.
    def reconnect
      purge_clients
      @zk ? @zk.reopen : setup_zk
      build_clients
    end

    # Retrieves the current redis master.
    #
    # @return [String] the host/port of the current master
    def current_master
      master = @lock.synchronize { @master }
      address_for(master)
    end

    # Retrieves the current redis slaves.
    #
    # @return [Array<String>] an array of known slave host/port addresses
    def current_slaves
      slaves = @lock.synchronize { @slaves }
      addresses_for(slaves)
    end

    private

    # Sets up the underlying ZooKeeper connection.
    def setup_zk
      @zk = ZK.new(@zkservers) if @zkservers
      @zk.register(redis_nodes_path) { |event| handle_zk_event(event) }
      if @safe_mode
        @zk.on_expired_session { purge_clients }
      end
      @zk.on_connected { @zk.stat(redis_nodes_path, :watch => true) }
      @zk.stat(redis_nodes_path, :watch => true)
      update_znode_timestamp
    end

    # Handles a ZK event.
    #
    # @param [ZK::Event] event the ZK event to handle
    def handle_zk_event(event)
      update_znode_timestamp
      if event.node_created? || event.node_changed?
        build_clients
      elsif event.node_deleted?
        purge_clients
        @zk.stat(redis_nodes_path, :watch => true)
      else
        logger.error("Unknown ZK node event: #{event.inspect}")
      end
    ensure
      @zk.stat(redis_nodes_path, :watch => true)
    end

    # Determines if a method is a known redis operation.
    #
    # @param [Symbol] method the method to check
    # @return [Boolean] true if redis operation, false otherwise
    def redis_operation?(method)
      Redis.public_instance_methods(false).include?(method)
    end

    # Dispatches a redis operation to a master or slave.
    #
    # @param [Symbol] method the method to dispatch
    # @param [Array] args the arguments to pass to the method
    # @param [Proc] block an optional block to pass to the method
    # @return [Object] the result of dispatching the command
    def dispatch(method, *args, &block)
      if @safe_mode && !recently_heard_from_node_manager?
        build_clients
      end

      verify_supported!(method)
      tries = 0
      begin
        client_for(method).send(method, *args, &block)
      rescue *CONNECTIVITY_ERRORS => ex
        logger.error("Error while handling `#{method}` - #{ex.inspect}")
        logger.error(ex.backtrace.join("\n"))

        if tries < @max_retries
          tries += 1
          free_client
          build_clients
          sleep(RETRY_WAIT_TIME)
          retry
        end
        raise
      ensure
        free_client
      end
    end

    # Returns the currently known master.
    #
    # @return [Redis] the Redis client for the current master
    # @raise [NoMasterError] if no master is available
    def master
      if master = @lock.synchronize { @master }
        verify_role!(master, :master) if @verify_role
        return master
      end
      raise NoMasterError
    end

    # Returns a random slave from the list of known slaves.
    #
    # @note If there are no slaves, the master is returned.
    # @return [Redis] the Redis client for the slave or master
    # @raise [NoMasterError] if no master fallback is available
    def slave
      # pick a slave, if none available fallback to master
      if slave = @lock.synchronize { @slaves.shuffle.first }
        verify_role!(slave, :slave) if @verify_role
        return slave
      end
      master
    end

    # Builds the Redis clients for the currently known master/slaves.
    # The current master/slaves are fetched via ZooKeeper.
    def build_clients
      @lock.synchronize do
        begin
          nodes = fetch_nodes
          return unless nodes_changed?(nodes)

          purge_clients
          logger.info("Building new clients for nodes #{nodes.inspect}")
          new_master = new_clients_for(nodes[:master]).first if nodes[:master]
          new_slaves = new_clients_for(*nodes[:slaves])
          @master = new_master
          @slaves = new_slaves
        rescue
          purge_clients
          raise
        ensure
          if should_notify?
           @on_node_change.call(current_master, current_slaves)
           @last_notified_master = current_master
           @last_notified_slaves = current_slaves
          end
        end
      end
    end

    # Determines if the on_node_change callback should be invoked.
    #
    # @return [Boolean] true if callback should be invoked, false otherwise
    def should_notify?
      return false unless @on_node_change
      return true if @last_notified_master != current_master
      return true if different?(Array(@last_notified_slaves), current_slaves)
      false
    end

    # Fetches the known redis nodes from ZooKeeper.
    #
    # @return [Hash] the known master/slave redis servers
    def fetch_nodes
      tries = 0
      begin
        data = @zk.get(redis_nodes_path, :watch => true).first
        nodes = symbolize_keys(decode(data))
        logger.debug("Fetched nodes: #{nodes.inspect}")
        nodes
      rescue Zookeeper::Exceptions::InheritedConnectionError, ZK::Exceptions::InterruptedSession => ex
        logger.info { "Caught #{ex.class} '#{ex.message}' - reopening ZK client" }
        @zk.reopen
        retry
      rescue *ZK_ERRORS => ex
        logger.warn { "Caught #{ex.class} '#{ex.message}' - retrying ..." }
        sleep(RETRY_WAIT_TIME)

        if tries < @max_retries
          tries += 1
          retry
        elsif tries < (@max_retries * 2)
          tries += 1
          logger.warn { "Hmmm, more than [#{@max_retries}] retries: reopening ZK client" }
          @zk.reopen
          retry
        else
          tries = 0
          logger.warn { "Oops, more than [#{@max_retries * 2}] retries: establishing fresh ZK client" }
          @zk.close!
          setup_zk
          retry
        end
      end
    end

    # Builds new Redis clients for the specified nodes.
    #
    # @param [Array<String>] nodes the array of redis host:port pairs
    # @return [Array<Redis>] the array of corresponding Redis clients
    def new_clients_for(*nodes)
      nodes.map do |node|
        host, port = node.split(':')
        opts = {:host => host, :port => port}
        opts.update(:db => @db) if @db
        opts.update(:password => @password) if @password
        client = Redis.new(@redis_client_options.merge(opts))
        if @namespace
          client = Redis::Namespace.new(@namespace, :redis => client)
        end
        @node_addresses[client] = node
        client
      end
    end

    # @return [String] a friendly name for current master
    def master_name
      address_for(@master) || 'none'
    end

    # @return [Array<String>] friendly names for current slaves
    def slave_names
      return 'none' if @slaves.empty?
      addresses_for(@slaves).join(', ')
    end

    # Verifies the actual role for a redis node.
    #
    # @param [Redis] node the redis node to check
    # @param [Symbol] role the role to verify
    # @return [Symbol] the verified role
    # @raise [InvalidNodeRoleError] if the role is invalid
    def verify_role!(node, role)
      current_role = node.info['role']
      if current_role.to_sym != role
        raise InvalidNodeRoleError.new(address_for(node), role, current_role)
      end
      role
    end

    # Ensures that the method is supported.
    #
    # @raise [UnsupportedOperationError] if the operation isn't supported
    def verify_supported!(method)
      if UNSUPPORTED_OPS.include?(method)
        raise UnsupportedOperationError.new(method)
      end
    end

    # Returns node addresses.
    #
    # @param [Array<Redis>] nodes the redis clients
    # @return [Array<String>] the addresses for the nodes
    def addresses_for(nodes)
      nodes.map { |node| address_for(node) }
    end

    # Returns a node address.
    #
    # @param [Redis] node a redis client
    # @return [String] the address for the node
    def address_for(node)
      return unless node
      @node_addresses[node]
    end

    # Determines if the currently known redis servers is different
    # from the nodes returned by ZooKeeper.
    #
    # @param [Array<String>] new_nodes the new redis nodes
    # @return [Boolean] true if nodes are different, false otherwise
    def nodes_changed?(new_nodes)
      return true if address_for(@master) != new_nodes[:master]
      return true if different?(addresses_for(@slaves), new_nodes[:slaves])
      false
    end

    # Disconnects one or more redis clients.
    #
    # @param [Array<Redis>] redis_clients the redis clients
    def disconnect(*redis_clients)
      redis_clients.each do |conn|
        if conn
          begin
            conn.client.disconnect
          rescue
            # best effort
          end
        end
      end
    end

    # Disconnects current redis clients.
    def purge_clients
      @lock.synchronize do
        logger.info("Purging current redis clients")
        disconnect(@master, *@slaves)
        @master = nil
        @slaves = []
        @node_addresses = {}
      end
    end

    # Updates timestamp when an event is received by the Node Manager.
    def update_znode_timestamp
      @last_znode_timestamp = Time.now
    end

    # @return [Boolean] indicates if we recently heard from the Node Manager
    def recently_heard_from_node_manager?
      return false unless @last_znode_timestamp
      Time.now - @last_znode_timestamp <= ZNODE_UPDATE_TIMEOUT
    end

    # Acquires a client to use for the specified operation.
    #
    # @param [Symbol] method the method for which to retrieve a client
    # @return [Redis] a redis client to use
    # @note
    #   This method stores a stack of clients used to handle the case
    #   where the same RedisFailover::Client instance is referenced by
    #   nested blocks (e.g., block passed to multi).
    def client_for(method)
      stack = Thread.current[@current_client_key] ||= []
      client = if stack.last
        stack.last
      elsif @master_only
        master
      elsif REDIS_READ_OPS.include?(method)
        slave
      else
        master
      end

      stack << client
      client
    end

    # Pops a client from the thread-local client stack.
    def free_client
      if stack = Thread.current[@current_client_key]
        stack.pop
      end
      nil
    end

    # Parses the configuration operations.
    #
    # @param [Hash] options the configuration options
    def parse_options(options)
      @zk, @zkservers = options.values_at(:zk, :zkservers)
      if [@zk, @zkservers].all? || [@zk, @zkservers].none?
        raise ArgumentError, 'must specify :zk or :zkservers'
      end

      @root_znode = options.fetch(:znode_path, Util::DEFAULT_ROOT_ZNODE_PATH)
      @namespace = options[:namespace]
      @password = options[:password]
      @db = options[:db]
      @retry = options.fetch(:retry_failure, true)
      @max_retries = @retry ? options.fetch(:max_retries, 3) : 0
      @safe_mode = options.fetch(:safe_mode, true)
      @master_only = options.fetch(:master_only, false)
      @verify_role = options.fetch(:verify_role, true)

      @redis_client_options = Redis::Client::DEFAULTS.keys.each_with_object({}) do |key, hash| 
        hash[key] = options[key]
      end
    end

    # @return [String] the znode path for the master redis nodes config
    def redis_nodes_path
      "#{@root_znode}/nodes"
    end
  end
end
