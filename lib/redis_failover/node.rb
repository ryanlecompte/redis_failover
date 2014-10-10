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

    # @return [String] the redis server host
    attr_reader :host

    # @return [Integer] the redis server port
    attr_reader :port

    # Creates a new instance.
    #
    # @param [Hash] options the options used to create the node
    # @option options [String] :host the host of the redis server
    # @option options [String] :port the port of the redis server
    def initialize(options = {})
      @host = options[:host]
      raise InvalidNodeError, 'missing host' if @host.to_s.empty?
      @port = Integer(options[:port] || 6379)
      @password = options[:password]
    end

    # @return [Boolean] true if this node is a master, false otherwise
    def master?
      role == 'master'
    end

    # @return [Boolean] true if this node is a slave, false otherwise
    def slave?
      !master?
    end

    # Determines if this node is a slave of the given master.
    #
    # @param [Node] master the master to check
    # @return [Boolean] true if slave of master, false otherwise
    def slave_of?(master)
      current_master == master
    end

    # Determines current master of this slave.
    #
    # @return [Node] the node representing the master of this slave
    def current_master
      info = fetch_info
      return unless info[:role] == 'slave'
      Node.new(:host => info[:master_host], :port => info[:master_port].to_i)
    end

    # Uses ECHO command since it properly fails when slave-serve-stale-data is disabled and slave is out of sync
    def healthcheck
      perform_operation do |redis|
        redis.echo(check_key)
      end
    end

    # Makes this node a slave of the given node.
    #
    # @param [Node] node the node of which to become a slave
    def make_slave!(node)
      perform_operation do |redis|
        unless slave_of?(node)
          redis.slaveof(node.host, node.port)
          logger.info("#{self} is now a slave of #{node}")
        end
      end
    end

    # Makes this node a master node.
    def make_master!
      perform_operation do |redis|
        unless master?
          redis.slaveof('no', 'one')
          logger.info("#{self} is now master")
        end
      end
    end

    # @return [String] an inspect string for this node
    def inspect
      "<RedisFailover::Node #{to_s}>"
    end

    # @return [String] a friendly string for this node
    def to_s
      "#{@host}:#{@port}"
    end

    # Determines if this node is equal to another node.
    #
    # @param [Node] other the other node to compare
    # @return [Boolean] true if equal, false otherwise
    def ==(other)
      return false unless Node === other
      return true if self.equal?(other)
      [host, port] == [other.host, other.port]
    end
    alias_method :eql?, :==

    # @return [Integer] a hash value for this node
    def hash
      to_s.hash
    end

    # Fetches information/stats for this node.
    #
    # @return [Hash] the info for this node
    def fetch_info
      perform_operation do |redis|
        symbolize_keys(redis.info)
      end
    end
    alias_method :ping, :fetch_info

    # @return [Boolean] determines if this node prohibits stale reads
    def prohibits_stale_reads?
      perform_operation do |redis|
        redis.config('get', 'slave-serve-stale-data').last == 'no'
      end
    end

    # @return [Boolean] determines if this node is syncing with its master
    def syncing_with_master?
      fetch_info[:master_sync_in_progress] == '1'
    end

    # @return [Integer] ranking of master-electability based on communication lag
    def electability
      info = fetch_info
      lag = if info[:master_sync_in_progress] == '1'   #protect from partial dataset when slave is mid-sync
        -1
      elsif info[:role] == "master"    # don't promote a node that already thinks its a (false) master
        -2
      else
        lag_time  = info[:master_last_io_seconds_ago]
        down_time = info[:master_link_down_since_seconds]

        if down_time && ( lag_time.nil? || lag_time.to_i < down_time.to_i )
          lag_time = down_time
        end

        lag_time
      end

      return lag.nil? ? -1 : lag.to_i
    end


    private

    # @return [String] the current role for this node
    def role
      fetch_info[:role]
    end

    # @return [String] the name of the healthcheck key for this node
    def check_key
      @check_key ||= "_redis_failover_#{SecureRandom.hex(32)}"
    end

    # @return [Redis] a new redis client instance for this node
    def new_client
      Redis.new(:host => @host, :password => @password, :port => @port)
    end

    # Safely performs a redis operation within a given timeout window.
    #
    # @yield [Redis] the redis client to use for the operation
    # @raise [NodeUnavailableError] if node is currently unreachable
    def perform_operation
      redis = nil
      Timeout.timeout(MAX_OP_WAIT_TIME) do
        redis = new_client
        yield redis
      end
    rescue Exception => ex
      raise NodeUnavailableError, "#{ex.class}: #{ex.message}", ex.backtrace
    ensure
      if redis
        begin
          redis.client.disconnect
        rescue Exception => ex
          raise NodeUnavailableError, "#{ex.class}: #{ex.message}", ex.backtrace
        end
      end
    end
  end
end
