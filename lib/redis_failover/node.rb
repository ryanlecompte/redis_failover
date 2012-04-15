module RedisFailover
  # Represents a redis node (master or slave).
  class Node
    include Util

    BLPOP_WAIT_TIME = 3

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

    def slave_of?(master)
      current_master == master
    end

    def current_master
      info = fetch_info
      return unless info[:role] == 'slave'
      Node.new(:host => info[:master_host], :port => info[:master_port].to_i)
    end

    # Waits until something interesting happens. If the connection
    # with this node dies, the blpop call will raise an error. If
    # the blpop call returns without error, then this will be due to
    # a graceful shutdown signaled by #stop_waiting or a timeout.
    def wait
      perform_operation do
        redis.blpop(wait_key, 0, BLPOP_WAIT_TIME)
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
          # send a stop waiting signal so that its watcher
          # can properly handle its state change
          stop_waiting
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

    private

    def role
      fetch_info[:role]
    end

    def wait_key
      @wait_key ||= "_redis_failover_#{SecureRandom.hex(32)}"
    end

    def redis
      @redis ||= Redis.new(:host => @host, :password => @password, :port => @port)
    rescue
      raise NodeUnavailableError.new(self)
    end

    def perform_operation
      yield
    rescue
      raise NodeUnavailableError.new(self)
    end
  end
end
