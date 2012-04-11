require 'set'
require 'open-uri'

module RedisFailover
  # Redis failover-aware client.
  class Client
    include Util

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
      :info,
      :keys,
      :lastsave,
      :lindex,
      :llen,
      :lrange,
      :mapped_hmget,
      :mapped_mget,
      :mget,
      :ping,
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

    # Creates a new failover redis client.
    def initialize(options = {})
      unless options.values_at(:host, :port).all?
        raise ArgumentError, ':host and :port options required'
      end

      Util.logger = options[:logger] if options[:logger]
      @namespace = options[:namespace]
      @password = options[:password]
      @retry = options[:retry_failure] || true
      @max_retries = options[:max_retries] || 5
      @registry_url = "http://#{options[:host]}:#{options[:port]}/redis_servers"
      @master = nil
      @slaves = []
      build_clients
    end

    def method_missing(method, *args, &block)
      if redis_operation?(method)
        dispatch(method)
      else
        super
      end
    end

    def respond_to?(method)
      redis_operation?(method) || super
    end

    private

    def redis_operation?(method)
      Redis.public_instance_methods(false).include?(method)
    end

    def dispatch(method)
      tries = 0
      begin
        if REDIS_READ_OPS.include?(method)
          # send read operations to a slave
          slave.send(method, *args, &block)
        else
          # direct everything else to master
          master.send(method, *args, &block)
        end
      rescue NoMasterError, *REDIS_ERRORS
        logger.error("No suitable node available for operation #{method}.")
        sleep(0.5)
        build_clients

        if @retry && tries < @max_retries
          tries += 1
          retry
        end

        raise
      end
    end

    def master
      @master or raise NoMasterError
    end

    def slave
      # pick a slave, if none available fallback to master
      @slaves.sample || master
    end

    def build_clients
      return if all_nodes_available?

      logger.info('Attempting to fetch nodes and build redis clients.')
      servers = fetch_redis_servers
      master = new_clients_for(servers[:master]).first if servers[:master]
      slaves = new_clients_for(*servers[:slaves])

      # once clients are successfully created, swap the references
      @master = master
      @slaves = slaves
    rescue => ex
      logger.error("Failed to fetch servers from #{@registry_url} - #{ex.message}")
      logger.error(ex.backtrace.join("\n"))
    end

    def fetch_redis_servers
      open(@registry_url) do |io|
        servers = symbolize_keys(MultiJson.decode(io))
        logger.info("Fetched servers: #{servers}")
        servers
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

    def all_nodes_available?
      [@master, *@slaves].all? do |client|
        client && client.info rescue false
      end
    end
  end
end
