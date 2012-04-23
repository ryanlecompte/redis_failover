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
      @lock = Monitor.new

      @on_session_expiration = @on_session_recovered = nil
      @session_expiration_sub = @session_recovered_sub = nil

      build_client
    end

    def on_session_expiration(&block)
      @lock.synchronize do
        @on_session_expiration = block
      end
    end

    def on_session_recovered(&block)
      @lock.synchronize do
        @on_session_recovered = block
      end
    end

    # method_missing?
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
    def handle_expired_session_event(*ignored)
      cb = @lock.synchronize { @on_session_expiration }
      cb.call if cb 
    end

    def handle_connected_event(*ignored)
      cb = @lock.synchronize { @on_session_recovered }
      cb.call if cb
    end

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
        @session_expiration_sub.unregister if @session_expiration_sub
        @session_recovered_sub.unregister  if @session_recovered_sub
        @client.close! if @client

        @client = ZK.new(@servers)

        @session_expiration_sub = @client.on_expired_session(&method(:handle_expired_session_event))
        @session_recovered_sub  = @client.on_connected(&method(:handle_connected_event))

        @setup_block.call(self) if @setup_block
        logger.info("Communicating with ZooKeeper servers #{@servers}")
      end
    end
  end
end
