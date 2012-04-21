module RedisFailover
  # ZkClient is a thin wrapper over the ZK client to gracefully handle reconnects
  # when a session expires.
  class ZkClient
    include Util

    # Time to sleep before retrying a failed operation.
    TIMEOUT = 2

    # Maximum reconnect attempts.
    MAX_RECONNECTS = 3

    # Errors that are candidates for rebuilding the underlying ZK client.
    RECONNECTABLE_ERRORS = [
      ZookeeperExceptions::ZookeeperException::SessionExpired,
      ZookeeperExceptions::ZookeeperException::SystemError,
      ZookeeperExceptions::ZookeeperException::ConnectionLoss,
      ZookeeperExceptions::ZookeeperException::OperationTimeOut,
      ZookeeperExceptions::ZookeeperException::AuthFailed,
      ZookeeperExceptions::ZookeeperException::SessionMoved,
      ZookeeperExceptions::ZookeeperException::ConnectionClosed,
      ZookeeperExceptions::ZookeeperException::NotConnected
    ].freeze

    # ZK methods that are wrapped with reconnect logic.
    WRAPPED_ZK_METHODS = [
      :get,
      :set,
      :watcher,
      :event_handler,
      :stat,
      :create,
      :delete,
      :with_lock].freeze

    def initialize(servers, &setup_block)
      @servers = servers
      @setup_block = setup_block
      @lock = Mutex.new
      build_client
    end

    def delegate
      @client
    end

    def on_session_expiration(&block)
      @client.on_expired_session { block.call }
      @on_session_expiration = block
    end

    def on_session_recovered(&block)
      @client.on_connected { block.call }
      @on_session_recovered = block
    end

    WRAPPED_ZK_METHODS.each do |zk_method|
      class_eval(<<-RUBY, __FILE__, __LINE__ + 1)
        def #{zk_method}(*args, &block)
          perform_with_reconnect do
            @client.#{zk_method}(*args, &block)
          end
        end
      RUBY
    end

    private

    def perform_with_reconnect
      tries = 0
      begin
        yield
      rescue *RECONNECTABLE_ERRORS => ex
        logger.error("ZooKeeper connection error, rebuilding client: #{ex.inspect}")
        logger.error(ex.backtrace.join("\n"))
        if tries < MAX_RECONNECTS
          tries += 1
          @on_session_expiration.call if @on_session_expiration
          build_client
          @on_session_recovered.call if @on_session_recovered
          sleep(TIMEOUT)
          retry
        end

        raise
      end
    end

    def build_client
      @lock.synchronize do
        @client = ZK.new(@servers)
        @setup_block.call(self) if @setup_block
        logger.info("Communicating with ZooKeeper servers #{@servers}")
      end
    end
  end
end
