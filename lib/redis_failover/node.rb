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

    def reachable?
      perform_operation do
        redis.ping
      end
      true
    rescue NodeUnreachableError
      false
    end

    def unreachable?
      !reachable?
    end

    def master?
      role == 'master'
    end

    def slave?
      !master?
    end

    def wait_until_unreachable
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
        redis.slaveof(master.host, master.port)
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

    private

    def role
      fetch_info[:role]
    end

    def fetch_info
      perform_operation do
        symbolize_keys(redis.info)
      end
    end
    alias_method :ping, :fetch_info

    def wait_key
      @wait_key ||= "_redis_failover_#{SecureRandom.hex(32)}"
    end

    def redis
      Redis.new(:host => @host, :password => @password, :port => @port)
    rescue
      raise NodeUnreachableError.new(self)
    end

    def perform_operation
      yield
    rescue
      raise NodeUnreachableError.new(self)
    end
  end
end
