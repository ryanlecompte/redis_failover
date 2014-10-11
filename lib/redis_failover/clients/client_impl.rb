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
  class ClientImpl
    include Util

    # Maximum allowed elapsed time between notifications from the Node Manager.
    # When this timeout is reached, the client will raise a NoNodeManagerError
    # and purge its internal redis clients.
    NODE_UPDATE_TIMEOUT = 9

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
    # @option options [String] :trace_id trace string tag logged for client debugging
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
      @trace_id = options[:trace_id]
      @master = nil
      @slaves = []
      @node_addresses = {}
      @lock = Monitor.new
      @current_client_key = "current-client-#{self.object_id}"
      yield self if block_given?

      parse_options(options)
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
      "#<RedisFailover::Client [#{@trace_id}] (db: #{@db.to_i}, master: #{master_name}, slaves: #{slave_names})>"
    end
    alias_method :to_s, :inspect

    # Force a manual failover to a new server. A specific server can be specified
    # via options. If no options are passed, a random slave will be selected as
    # the candidate for the new master.
    #
    # @param [Hash] options the options used for manual failover
    # @option options [String] :host the host of the failover candidate
    # @option options [String] :port the port of the failover candidate
    def manual_failover(db_client, root_node, options = {})
      ManualFailover.new(db_client, root_node, options).perform
      self
    end

    # Gracefully performs a shutdown of this client. This method is
    # mostly useful when the client is used in a forking environment.
    # When a fork occurs, you can call this method in an after_fork hook,
    # and then create a new instance of the client. The underlying
    # ZooKeeper client and redis clients will be closed.
    def shutdown
      raise StandardError, 'Error: `shutdown` needs to be implemented in child class.'
    end

    # Reconnect method needed for compatibility with 3rd party libs (i.e. Resque) that expect this for redis client objects.
    def reconnect
      raise StandardError, 'Error: `reconnect` needs to be implemented in child class.'
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
        #logger.info("#{method}: #{(Thread.current[@current_client_key] ||= []).map(&:object_id)}")
        redis = client_for(method)
        redis.send(method, *args, &block)
      rescue ::Redis::InheritedError => ex
        logger.debug( "Caught #{ex.class} - reconnecting [#{@trace_id}] #{redis.inspect}" )
        free_client
        redis.client.reconnect
        retry
      rescue *CONNECTIVITY_ERRORS => ex
        logger.error("Error while handling `#{method}` - #{ex.inspect}: #{ex.backtrace.join("\n")}")

        if tries < @max_retries
          tries += 1
          free_client
          sleep(RETRY_WAIT_TIME)
          build_clients
          retry
        end
        raise
      ensure
        free_client
      end
    end

    # Builds the Redis clients for the currently known master/slaves.
    # The current master/slaves are fetched via ZooKeeper or Etcd.
    def build_clients(nodes = nil)
      @lock.synchronize do
        begin
          nodes ||= fetch_nodes
          return unless nodes_changed?(nodes)

          purge_clients
          logger.info("Building new clients for nodes [#{@trace_id}] #{nodes.inspect}")
          new_master = new_clients_for(nodes[:master]).first if nodes[:master]
          new_slaves = new_clients_for(*nodes[:slaves])
          @master = new_master
          @slaves = new_slaves
          logger.info("New configuration for [#{@trace_id}]: #{configuration_to_s}")
        rescue => ex
          logger.info("Error while builing clients: #{ex.message}, repurging...")
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

    def configuration_to_s
      "master(#{master_name}), slaves(#{slave_names})"
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

    # Determines if the on_node_change callback should be invoked.
    #
    # @return [Boolean] true if callback should be invoked, false otherwise
    def should_notify?
      return false unless @on_node_change
      return true if @last_notified_master != current_master
      return true if different?(Array(@last_notified_slaves), current_slaves)
      false
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
        logger.info("Purging current redis clients [#{@trace_id}] for #{configuration_to_s}")
        disconnect(@master, *@slaves)
        @master = nil
        @slaves = []
        @node_addresses = {}
      end
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

    # @return [Boolean] indicates if we recently heard from the Node Manager
    def recently_heard_from_node_manager?
      return false unless @last_node_timestamp
      Time.now - @last_node_timestamp <= NODE_UPDATE_TIMEOUT
    end

    # Pops a client from the thread-local client stack.
    def free_client
      if stack = Thread.current[@current_client_key]
        stack.pop
      end
      nil
    end

    # Updates timestamp when an event is received by the Node Manager.
    def update_node_timestamp
      @last_node_timestamp = Time.now
    end

    def parse_redis_options(options)
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
      "#{@root_node}/nodes"
    end
  end
end
