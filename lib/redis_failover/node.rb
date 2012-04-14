module RedisFailover
  # Represents a redis node (master or slave).
  class Node
    include Util

    attr_reader :host, :port

    def initialize(options = {})
      @host = options.fetch(:host) { raise InvalidNodeError, 'missing host'}
      @port = Integer(options[:port] || 6379)
      @password = options[:password]
    end

    def master?
      role == 'master'
    end

    def slave?
      !master?
    end

    def syncing?
      prohibits_stale_reads? && syncing_with_master?
    end

    def wait_until_unavailable
      perform_operation do
        redis.blpop(wait_key, 0)
        redis.del(wait_key)
      end
    end

    def stop_waiting
      perform_operation do
        redis.lpush(wait_key, '1')
      end
    end

    def make_slave!(master)
      perform_operation do
        unless slave_of?(master)
          redis.slaveof(master.host, master.port)
        end
      end
    end

    def make_master!
      perform_operation do
        # yes, this is a real redis operation!
        redis.slaveof('no', 'one')
      end
    end

    def inspect
      "<RedisFailover::Node #{to_s}>"
    end

    def to_s
      "#{@host}:#{@port}"
    end

    def ==(other)
      return false unless other.is_a?(Node)
      return true if self.equal?(other)
      [host, port] == [other.host, other.port]
    end
    alias_method :eql?, :==

    def hash
      to_s.hash
    end

    def fetch_info
      perform_operation do
        symbolize_keys(redis.info)
      end
    end
    alias_method :ping, :fetch_info

    private

    def role
      fetch_info[:role]
    end

    def wait_key
      @wait_key ||= "_redis_failover_#{SecureRandom.hex(32)}"
    end

    def redis
      Redis.new(:host => @host, :password => @password, :port => @port)
    rescue
      raise NodeUnavailableError.new(self)
    end

    def perform_operation
      yield
    rescue
      raise NodeUnavailableError.new(self)
    end

    def slave_of?(master)
      info = fetch_info
      info[:role] == 'slave' &&
      info[:master_host] == master.host &&
      info[:master_port] == master.port.to_s
    end

    def prohibits_stale_reads?
      perform_operation do
        redis.config('get', 'slave-serve-stale-data').last == 'no'
      end
    end

    def syncing_with_master?
      perform_operation do
        fetch_info[:master_sync_in_progress] == '1'
      end
    end
  end
end
