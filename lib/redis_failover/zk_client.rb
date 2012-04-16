module RedisFailover
  # ZkClient is a thin wrapper over the ZK client to gracefully handle reconnects
  # when a session expires.
  class ZkClient
    include Util

    MAX_RECONNECTS = 3

    def initialize(servers)
      @servers = servers
      @lock = Mutex.new
      build_client
    end

    def on_session_expiration(&block)
      @client.on_expired_session { block.call }
      @on_session_expiration = block
    end

    def get(*args, &block)
      perform_with_reconnect { @client.get(*args, &block) }
    end

    def set(*args, &block)
      perform_with_reconnect { @client.set(*args, &block) }
    end

    def watcher(*args, &block)
      perform_with_reconnect { @client.watcher(*args, &block) }
    end

    def event_handler(*args, &block)
      perform_with_reconnect { @client.event_handler(*args, &block) }
    end

    def stat(*args, &block)
      perform_with_reconnect { @client.stat(*args, &block) }
    end

    def create(*args, &block)
      perform_with_reconnect { @client.create(*args, &block) }
    end

    def delete(*args, &block)
      perform_with_reconnect { @client.delete(*args, &block) }
    end

    private

    def perform_with_reconnect
      tries = 0
      begin
        yield
      rescue ZookeeperExceptions::ZookeeperException::SessionExpired
        logger.info("Zookeeper client session expired, rebuilding client.")
        if tries < MAX_RECONNECTS
          tries += 1
          build_client
          @on_session_expiration.call if @on_session_expiration
          sleep(2) && retry
        end

        raise
      end
    end

    def build_client
      @lock.synchronize do
        if @client
          @client.reopen
        else
          @client = ZK.new(@servers)
        end
        logger.info("Communicating with zookeeper servers #{@servers}")
      end
    end
  end
end
