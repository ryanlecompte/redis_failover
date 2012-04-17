require 'redis_failover/errors'

module RedisFailover
  # Common utiilty methods.
  module Util
    extend self

    DEFAULT_ZNODE_PATH = '/redis_failover_nodes'
    REDIS_ERRORS = Errno.constants.map { |c| Errno.const_get(c) }
    ALL_ERRORS = [
      RedisFailover::Error,
      ZookeeperExceptions::ZookeeperException,
      REDIS_ERRORS,
      StandardError].flatten

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
