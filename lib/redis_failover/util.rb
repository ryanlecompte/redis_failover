require 'redis_failover/errors'

module RedisFailover
  # Common utiilty methods and constants.
  module Util
    extend self

    # Default node in ZooKeeper that contains the current list of available redis nodes.
    DEFAULT_ZNODE_PATH = '/redis_failover_nodes'.freeze

    # Connectivity errors that the redis client raises.
    REDIS_ERRORS = Errno.constants.map { |c| Errno.const_get(c) }.freeze

    # Full set of errors related to connectivity.
    CONNECTIVITY_ERRORS = [
      RedisFailover::Error,
      ZK::Exceptions::KeeperException,
      ZookeeperExceptions::ZookeeperException,
      REDIS_ERRORS].flatten.freeze

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
  end
end
