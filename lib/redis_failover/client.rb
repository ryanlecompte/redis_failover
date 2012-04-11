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

    def initialize(options = {})
      unless options.values_at(:host, :port).all?
        raise ArgumentError, ':host and :port options required'
      end

      Util.logger = options[:logger] if options[:logger]
      @password = options[:password]
      @retry = options[:retry_failure] || true
      @registry_url = "http://#{options[:host]}:#{options[:port]}/redis_servers"
      @master = nil
      @slaves = []
      build_clients
    end

    def method_missing(meth, *args, &block)
      if read_operation?(meth)
        # send read operations to a slave
        slave.send(meth, *args, &block)
      else
        # direct everything else to master
        master.send(meth, *args, &block)
      end
    rescue NoMasterError, NoNodeAvailableError, *REDIS_ERRORS
      logger.error("No suitable node available for operation #{meth}.")
      # sleep for a bit and rebuild clients
      sleep(0.5)
      build_clients
      @retry ? retry : raise
    end

    private

    def master
      @master or raise NoMasterError
    end

    def slave
      @slaves.sample or master or raise NoNodeAvailableError
    end

    def fetch_redis_servers
      open(@registry_url) do |io|
        servers = symbolize_keys(MultiJson.decode(io))
        logger.info("Fetched servers: #{servers}")
        servers
      end
    rescue => ex
      logger.error("Failed to fetch servers from #{@registry_url} - #{ex.message}")
      logger.error(ex.backtrace.join("\n"))
      nil
    end

    def build_clients
      return if all_nodes_available?

      logger.info('Attempting to fetch nodes and build redis clients.')
      if servers = fetch_redis_servers
        master = new_clients(servers[:master]).first if servers[:master]
        slaves = new_clients(*servers[:slaves])

        # once clients are created, swap the references
        @master = master
        @slaves = slaves
      end
    end

    def new_clients(*nodes)
      nodes.map do |node|
        host, port = node.split(':')
        Redis.new(:host => host, :port => port, :password => @password)
      end
    end

    def read_operation?(op)
      REDIS_READ_OPS.include?(op)
    end

    def all_nodes_available?
      [@master, *@slaves].all? do |client|
        client.info rescue false
      end
    end
  end
end
