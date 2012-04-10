module RedisFailover
  # Common utiilty methods.
  module Util
    extend self

    def symbolize_keys(hash)
      Hash[hash.map { |k, v| [k.to_sym, v] }]
    end

    def logger
      @logger ||= Logger.new(STDOUT)
    end
  end
end
