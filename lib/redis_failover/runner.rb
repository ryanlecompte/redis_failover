module RedisFailover
  # Runner is responsible for bootstrapping the redis failover server.
  class Runner
    def self.run(options)
      options = CLI.parse(options)
      Util.logger.info("Redis Failover Server starting on port #{options[:port]}")
      Server.set(:port, options[:port])
      @node_manager = NodeManager.new(options)
      server_thread = Thread.new { Server.run! { |server| trap_signals } }
      node_manager_thread = Thread.new { @node_manager.start }
      [server_thread, node_manager_thread].each(&:join)
    end

    def self.node_manager
      @node_manager
    end

    def self.trap_signals
      [:INT, :TERM].each do |signal|
        previous_signal = trap(signal) do
          Util.logger.info('Shutting down ...')
          if previous_signal && previous_signal.respond_to?(:call)
            previous_signal.call
          end
          @node_manager.shutdown
          exit(0)
        end
      end
    end
  end
end
