# Test stub for Redis.
class RedisStub
  class Proxy
    def initialize(opts = {})
      @info = {:role => 'master'}
      @queue = Queue.new
    end

    def blpop(*args)
      @queue.pop
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
  end

  def initialize(opts = {})
    @proxy = Proxy.new(opts)
    @reachable = true
  end

  def method_missing(m, *args, &block)
    if @reachable
      @proxy.send(m, *args, &block)
    else
      raise RuntimeError, 'failed to connect to redis'
    end
  end

  def make_unreachable!
    @reachable = false
  end
end

module RedisStubSupport
  def redis
    @redis ||= RedisStub.new(:host => @host, :port => @port)
  end
end
