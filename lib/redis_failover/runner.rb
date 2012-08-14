module RedisFailover
  # Runner is responsible for bootstrapping the Node Manager.
  class Runner
    # Launches the Node Manager in a background thread.
    #
    # @param [Array] options the command-line options
    # @note this method blocks and does not return until the
    #   Node Manager is gracefully stopped
    def self.run(options)
      options = CLI.parse(options)
      @node_manager = NodeManager.new(options)
      trap_signals
      @node_manager_thread = Thread.new { @node_manager.start }
      @node_manager_thread.join
    end

    # Traps shutdown signals.
    def self.trap_signals
      [:INT, :TERM].each do |signal|
        trap(signal) do
          Util.logger.info('Shutting down ...')
          @node_manager.shutdown
          @node_manager_thread.join
          exit(0)
        end
      end
    end
  end
end
