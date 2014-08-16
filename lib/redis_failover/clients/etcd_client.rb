require_relative 'client_impl'
Dir["#{File.dirname(__FILE__)}/../etcd_utils/*.rb"].each {|file| require file }
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
  #   etcd_servers = 'localhost:4001,localhost:4002,localhost:4003'
  #   client = RedisFailover::Client.new(:etcd_servers => etcd_servers)
  #   client.set('foo', 1) # will be directed to master
  #   client.get('foo') # will be directed to a slave
  #
  class EtcdClient < ClientImpl
    include RedisFailover::EtcdClientHelper
    # Creates a new failover redis client.
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
      super(@etcd, @root_node, options)
    end

    # Gracefully performs a shutdown of this client. This method is
    # mostly useful when the client is used in a forking environment.
    # When a fork occurs, you can call this method in an after_fork hook,
    # and then create a new instance of the client. The underlying
    # ZooKeeper client and redis clients will be closed.
    def shutdown
      terminte_threads
      @etcd = nil
      purge_clients
    end

    # Reconnect method needed for compatibility with 3rd party libs (i.e. Resque) that expect this for redis client objects.
    def reconnect
      # No persistant connection with Etcd
    end

    private

    # Parses the configuration operations.
    #
    # @param [Hash] options the configuration options
    def parse_options(options)
      if options[:etcd] && options[:etcd].empty?
        raise ArgumentError, 'must specify etcd option using `:etcd`'
      end

      @root_node = options[:node_path] || options[:znode_path] || Util::DEFAULT_ROOT_NODE_PATH
      parse_redis_options(options)
    end

    # Configures the Etcd client.
    def setup_etcd
      return unless @etcd

      @etcd = Etcd.client(@options[:etcd])
      @etcd.watch_etcd_folder(redis_nodes_path) {|response| handle_etcd_event(response) }
    end

    # Handles a Etcd event.
    #
    # @param [Etcd::Response] response the Etcd event to handle
    def handle_etcd_event(response)
      @last_node_timestamp = Time.now

      if response.action == "set" || response.action == "create"
        build_clients(redis_nodes_from_response(response))
      elsif esponse.action == "delete"
        purge_clients
      else
        logger.error("Unknown ETCD node event: #{response.inspect}")
      end
    end

    def redis_nodes_from_response(response)
      value = response.node && response.node.value
      begin
         symbolize_keys(decode(value))
      rescue
        logger.error("Error redis node not in a proper format in the response: #{response.inspect}")
        symbolize_keys(value) rescue value
      end
    end

    # Fetches the known redis nodes from Etcd.
    #
    # @return [Hash] the known master/slave redis servers
    def fetch_nodes
      tries = 0
      begin
        response = @etcd.get(redis_nodes_path)
        nodes = redis_nodes_from_response(response)
        logger.debug("Fetched nodes: #{nodes.inspect}")
        nodes
      rescue *ETCD_ERRORS => ex
        logger.error { "Caught #{ex.class} '#{ex.message}' - retrying ... [#{@trace_id}]" }
        sleep(RETRY_WAIT_TIME)

        if (tries += 1) <= @max_retries
          retry
        else
          tries = 0
          logger.error { "Oops, more than [#{@max_retries * 2}] retries: establishing fresh ETCD client [#{@trace_id}]" }
          terminte_threads
          @etcd = nil
          setup_etcd
          retry
        end
      end
    end
  end
end
