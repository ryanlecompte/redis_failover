module RedisFailover
  # Common utiilty methods.
  module Util
    extend self

    def symbolize_keys(hash)
      Hash[hash.map { |k, v| [k.to_sym, v] }]
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
  end
end
