require 'redis_failover/errors'

module RedisFailover
  # Common utiilty methods and constants.
  module Util
    extend self

    # Default node in ZK that contains the current list of available redis nodes.
    DEFAULT_ZNODE_PATH = '/redis_failover_nodes'.freeze

    # Connectivity errors that the redis (<3.x) client raises.
    REDIS_ERRORS = Errno.constants.map { |c| Errno.const_get(c) }

    # Connectivity errors that the redis (>3.x) client raises.
    REDIS_ERRORS << Redis::BaseError if Redis.const_defined?("BaseError")
    REDIS_ERRORS.freeze

    # Full set of errors related to connectivity.
    CONNECTIVITY_ERRORS = [
      RedisFailover::Error,
      ZK::Exceptions::InterruptedSession,
      REDIS_ERRORS
    ].flatten.freeze

    # Symbolizes the keys of the specified hash.
    #
    # @param [Hash] hash a hash for which keys should be symbolized
    # @return [Hash] a new hash with symbolized keys
    def symbolize_keys(hash)
      Hash[hash.map { |k, v| [k.to_sym, v] }]
    end

    # Determines if two arrays are different.
    #
    # @param [Array] ary_a the first array
    # @param [Array] ary_b the second array
    # @return [Boolean] true if arrays are different, false otherwise
    def different?(ary_a, ary_b)
      ((ary_a | ary_b) - (ary_a & ary_b)).size > 0
    end

    # @return [Logger] the logger instance to use
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

    # Sets a new logger to use.
    #
    # @param [Logger] logger a new logger to use
    def self.logger=(logger)
      @logger = logger
    end

    # @return [Logger] the logger instance to use
    def logger
      Util.logger
    end

    # Encodes the specified data in JSON format.
    #
    # @param [Object] data the data to encode
    # @return [String] the JSON-encoded data
    def encode(data)
      MultiJson.encode(data)
    end

    # Decodes the specified JSON data.
    #
    # @param [String] data the JSON data to decode
    # @return [Object] the decoded data
    def decode(data)
      return unless data
      MultiJson.decode(data)
    end
  end
end
