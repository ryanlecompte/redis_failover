require 'set'
require 'open-uri'

module RedisFailover
  # Redis failover-aware client.
  class Client
    include Util

    RETRY_WAIT_TIME = 3
    REDIS_ERRORS = Errno.constants.map { |c| Errno.const_get(c) }.freeze
    REDIS_READ_OPS = Set[
      :dbsize,
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
      :select,
      :sinter,
      :sismember,
      :smembers,
      :srandmember,
      :strlen,
      :sunion,
      :ttl,
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
    #   :host - redis failover server host (required)
    #   :port - redis failover server port (required)
    #   :password - optional password for redis nodes
    #   :namespace - optional namespace for redis nodes
    #   :logger - optional logger override
    #   :retry_failure - indicate if failures should be retried (default true)
    #   :max_retries - max retries for a failure (default 5)
    #
    def initialize(options = {})
      unless options.values_at(:host, :port).all?
        raise ArgumentError, ':host and :port options required'
      end

      Util.logger = options[:logger] if options[:logger]
      @namespace = options[:namespace]
      @password = options[:password]
      @retry = options[:retry_failure] || true
      @max_retries = @retry ? options.fetch(:max_retries, 3) : 0
      @server_url = "http://#{options[:host]}:#{options[:port]}/redis_servers"
      @master = nil
      @slaves = []
      build_clients
      start_background_monitor
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

    def redis_operation?(method)
      Redis.public_instance_methods(false).include?(method)
    end

    def dispatch(method, *args, &block)
      tries = 0

      begin
        if REDIS_READ_OPS.include?(method)
          # send read operations to a slave
          slave.send(method, *args, &block)
        else
          # direct everything else to master
          master.send(method, *args, &block)
        end
      rescue Error, *REDIS_ERRORS
        logger.error("No suitable node available for operation `#{method}.`")
        build_clients

        if tries < @max_retries
          tries += 1
          sleep(RETRY_WAIT_TIME) && retry
        end

        raise
      end
    end

    def master
      master = @master
      if master
        verify_role!(master, :master)
        return master
      end
      raise NoMasterError
    end

    def slave
      # pick a slave, if none available fallback to master
      if slave = @slaves.sample
        verify_role!(slave, :slave)
        return slave
      end
      master
    end

    def build_clients
      tries = 0

      begin
        logger.info('Checking for new redis nodes.')
        nodes = fetch_nodes
        return unless nodes_changed?(nodes)

        logger.info('Node change detected, rebuilding clients.')
        master = new_clients_for(nodes[:master]).first if nodes[:master]
        slaves = new_clients_for(*nodes[:slaves])

        # once clients are successfully created, swap the references
        @master = master
        @slaves = slaves
      rescue => ex
        logger.error("Failed to fetch nodes from #{@server_url} - #{ex.message}")
        logger.error(ex.backtrace.join("\n"))

        if tries < @max_retries
          tries += 1
          sleep(RETRY_WAIT_TIME) && retry
        end

        raise FailoverServerUnavailableError.new(@server_url)
      end
    end

    def fetch_nodes
      open(@server_url) do |io|
        nodes = symbolize_keys(MultiJson.decode(io))
        logger.info("Fetched nodes: #{nodes}")
        nodes
      end
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

    # Spawns a background thread to periodically fetch the latest
    # set of nodes from the redis failover server.
    def start_background_monitor
      Thread.new do
        loop do
          sleep(10)
          begin
            build_clients
          rescue => ex
            logger.error("Failed to poll for new nodes from #{@server_url} - #{ex.message}")
            logger.error(ex.backtrace.join("\n"))
          end
        end
      end
    end
  end
end
