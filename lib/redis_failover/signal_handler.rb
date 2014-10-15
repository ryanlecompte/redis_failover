module RedisFailover
  class SignalsHandler
    def initialize
      @reader, @writer = IO.pipe
      @queue = []
    end

    def listen_signal
      @listen_thread ||= Thread.new do
        loop do
          signal = self.pop
          yield(signal) if block_given?
        end
      end

      @listen_thread.abort_on_exception = true
    end

    def close
      @reader.close
      @writer.close
      @reader = @writer = nil
    end

    def closed?
      @reader.nil?
    end

    def push(item)
      raise RuntimeError, "SignalHandler's pipe is closed" if closed?
      @queue << item
      awaken
    end

    def pop(options = {})
      raise RuntimeError, "closed" if closed?
      if @queue.empty? && (:timeout == hibernate(options[:timeout]))
        :timeout
      else
        @queue.shift
      end
    end

    def awaken
      @writer.write '.'
    end

    def hibernate(seconds = nil)
      return :timeout unless IO.select([@reader], nil, nil, seconds)
      @reader.readchar
    end
  end
end