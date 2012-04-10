module RedisFailover
  # Represents a redis node (master or slave).
  class Node
    include Util

    attr_reader :host, :port

    def initialize(manager, options = {})
      @host = options.fetch(:host) { raise InvalidNodeError, 'missing host'}
      @port = options.fetch(:port, 6379)
      @password = options[:password]
      @manager = manager
    end

    def reachable?
      fetch_info
      true
    rescue
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
      redis.blpop(wait_key, 0)
      redis.del(wait_key)
    rescue
      unless reachable?
        raise NodeUnreachableError, 'failed while waiting'
      end
    end

    def stop_waiting
      redis.lpush(wait_key, '1')
    end

    def make_slave!
      master = @manager.current_master
      redis.slaveof(master.host, master.port)
    end

    def make_master!
      redis.slaveof('no', 'one')
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
      symbolize_keys(redis.info)
    end

    def wait_key
      @wait_key ||= "_redis_failover_#{SecureRandom.hex(32)}"
    end

    def redis
      Redis.new(:host => @host, :password => @password, :port => @port)
    rescue
      raise NodeUnreachableError, 'failed to create redis client'
    end
  end
end
