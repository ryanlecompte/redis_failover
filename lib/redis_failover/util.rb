module RedisFailover
  # Common utiilty methods.
  module Util
    extend self

    ZK_PATH = '/redis_failover_nodes'

    def symbolize_keys(hash)
      Hash[hash.map { |k, v| [k.to_sym, v] }]
    end

    def different?(ary_a, ary_b)
      ((ary_a | ary_b) - (ary_a & ary_b)).size > 0
    end

    def self.logger
      @logger ||= begin
        logger = Logger.new(STDOUT)
        logger.level = Logger::INFO
        logger.formatter = proc do |severity, datetime, progname, msg|
          "#{datetime.utc} RedisFailover #{Process.pid} #{severity}: #{msg}\n"
        end
        logger
      end
    end

    def self.logger=(logger)
      @logger = logger
    end

    def logger
      Util.logger
    end

    def encode(data)
      MultiJson.encode(data)
    end

    def decode(data)
      return unless data
      MultiJson.decode(data)
    end

    def new_zookeeper_client(servers)
      client = Zookeeper.new(servers)
      if client.state != Zookeeper::ZOO_CONNECTED_STATE
        raise ZookeeperError, "Not in connected state, client: #{client}"
      end
      logger.info("Communicating with zookeeper servers #{servers}")
      client
    rescue => ex
      raise ZookeeperError, "Failed to connect, error: #{ex.message}"
    end

    def zk_operation_failed?(result)
      result[:rc] != Zookeeper::ZOK
    end
  end
end
