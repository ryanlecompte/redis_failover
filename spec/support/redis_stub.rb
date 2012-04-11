require 'ostruct'

module RedisFailover
  # Test stub for Redis.
  class RedisStub
    class Proxy
      def initialize(queue, opts = {})
        @info = {:role => 'master'}
        @queue = queue
      end

      def blpop(*args)
        @queue.pop.tap do |value|
          raise value if value
        end
      end

      def del(*args)
      end

      def lpush(*args)
        @queue << nil
      end

      def slaveof(host, port)
        if host == 'no' && port == 'one'
          @info[:role] = 'master'
        else
          @info[:role] = 'slave'
        end
      end

      def info
        @info.dup
      end

      def ping
        'pong'
      end
    end

    attr_reader :host, :port, :reachable
    def initialize(opts = {})
      @host = opts[:host]
      @port = Integer(opts[:port])
      @queue = Queue.new
      @proxy = Proxy.new(@queue, opts)
      @reachable = true
    end

    def method_missing(method, *args, &block)
      if @reachable
        @proxy.send(method, *args, &block)
      else
        raise Errno::ECONNREFUSED
      end
    end

    def make_reachable!
      @reachable = true
    end

    def make_unreachable!
      @queue << Errno::ECONNREFUSED
      @reachable = false
    end

    def to_s
      "#{@host}:#{@port}"
    end

    def client
      OpenStruct.new(:host => @host, :port => @port)
    end
  end

  module RedisStubSupport
    def redis
      @redis ||= RedisStub.new(:host => @host, :port => @port)
    end
  end
end