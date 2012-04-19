module RedisFailover
  # Represents a redis node (master or slave). Instances of this class
  # are used by the NodeManager and NodeWatcher to manipulate real redis
  # servers.
  class Node
    include Util

    # Maximum amount of time given for any redis operation to complete.
    # If a redis operation doesn't complete in the alotted time, a
    # NodeUnavailableError will be raised.
    MAX_OP_WAIT_TIME = 5

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
    # a graceful shutdown signaled by #wakeup or a timeout.
    def wait
      perform_operation do |redis|
        redis.blpop(wait_key, MAX_OP_WAIT_TIME - 3)
        redis.del(wait_key)
      end
    end

    def wakeup
      perform_operation do |redis|
        redis.lpush(wait_key, '1')
      end
    end

    def make_slave!(master)
      perform_operation do |redis|
        unless slave_of?(master)
          redis.slaveof(master.host, master.port)
          logger.info("#{self} is now a slave of master #{master}")
          wakeup
        end
      end
    end

    def make_master!
      perform_operation do |redis|
        unless master?
          redis.slaveof('no', 'one')
          logger.info("#{self} is now master")
          wakeup
        end
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
      perform_operation do |redis|
        symbolize_keys(redis.info)
      end
    end
    alias_method :ping, :fetch_info

    def prohibits_stale_reads?
      perform_operation do |redis|
        redis.config('get', 'slave-serve-stale-data').last == 'no'
      end
    end

    def syncing_with_master?
      perform_operation do |redis|
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

    def new_client
      Redis.new(:host => @host, :password => @password, :port => @port)
    end

    def perform_operation
      redis = nil
      Timeout.timeout(MAX_OP_WAIT_TIME) do
        redis = new_client
        yield redis
      end
    rescue
      raise NodeUnavailableError.new(self)
    ensure
      if redis
        begin
          redis.client.disconnect
        rescue
          raise NodeUnavailableError.new(self)
        end
      end
    end
  end
end
