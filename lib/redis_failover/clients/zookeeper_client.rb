require_relative 'client_impl'
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
  class ZookeeperClient < ClientImpl

    # Maximum allowed elapsed time between notifications from the Node Manager.
    # When this timeout is reached, the client will raise a NoNodeManagerError
    # and purge its internal redis clients.
    ZNODE_UPDATE_TIMEOUT = 9

    # Creates a new failover redis client.
    #
    # @note Use either :zkservers or :zk
    # @return [RedisFailover::Client]
    def initialize(options = {})
      super
      setup_zk
      build_clients
    end

    # Force a manual failover to a new server. A specific server can be specified
    # via options. If no options are passed, a random slave will be selected as
    # the candidate for the new master.
    #
    # @param [Hash] options the options used for manual failover
    # @option options [String] :host the host of the failover candidate
    # @option options [String] :port the port of the failover candidate
    def manual_failover(options = {})
      super(@zk, @root_znode, options)
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

    # Reconnect method needed for compatibility with 3rd party libs (i.e. Resque) that expect this for redis client objects.
    def reconnect
      #We auto-detect underlying zk & redis client Inherited Error's and reconnect automatically as needed.
    end

    private

    # Parses the configuration operations.
    #
    # @param [Hash] options the configuration options
    def parse_options(options)
      @zk, @zkservers = options.values_at(:zk, :zkservers)
      if [@zk, @zkservers].all? || [@zk, @zkservers].none?
        raise ArgumentError, 'must specify :zk or :zkservers'
      end

      @root_node = options[:node_path] || options[:znode_path] || Util::DEFAULT_ROOT_NODE_PATH
      parse_redis_options(options)
    end

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

    # Builds the Redis clients for the currently known master/slaves.
    # The current master/slaves are fetched via ZooKeeper.
    def build_clients
      @lock.synchronize do
        begin
          nodes = fetch_nodes
          return unless nodes_changed?(nodes)

          purge_clients
          logger.info("Building new clients for nodes [#{@trace_id}] #{nodes.inspect}")
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
        logger.debug { "Caught #{ex.class} '#{ex.message}' - reopening ZK client [#{@trace_id}]" }
        sleep 1 if ex.kind_of?(ZK::Exceptions::InterruptedSession)
        @zk.reopen
        retry
      rescue *ZK_ERRORS => ex
        logger.error { "Caught #{ex.class} '#{ex.message}' - retrying ... [#{@trace_id}]" }
        sleep(RETRY_WAIT_TIME)

        if tries < @max_retries
          tries += 1
          retry
        elsif tries < (@max_retries * 2)
          tries += 1
          logger.error { "Hmmm, more than [#{@max_retries}] retries: reopening ZK client [#{@trace_id}]" }
          @zk.reopen
          retry
        else
          tries = 0
          logger.error { "Oops, more than [#{@max_retries * 2}] retries: establishing fresh ZK client [#{@trace_id}]" }
          @zk.close!
          setup_zk
          retry
        end
      end
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

    # Updates timestamp when an event is received by the Node Manager.
    def update_znode_timestamp
      @last_znode_timestamp = Time.now
    end

    # @return [Boolean] indicates if we recently heard from the Node Manager
    def recently_heard_from_node_manager?
      return false unless @last_znode_timestamp
      Time.now - @last_znode_timestamp <= ZNODE_UPDATE_TIMEOUT
    end

    # Pops a client from the thread-local client stack.
    def free_client
      if stack = Thread.current[@current_client_key]
        stack.pop
      end
      nil
    end
  end
end
