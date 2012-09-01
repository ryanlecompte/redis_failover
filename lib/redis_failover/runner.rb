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
      node_manager = NodeManager.new(options)
      trap_signals(node_manager)
      node_manager.start
    end

    # Traps shutdown signals.
    # @param [NodeManager] node_manager the node manager
    def self.trap_signals(node_manager)
      [:INT, :TERM].each do |signal|
        trap(signal) do
          node_manager.shutdown
        end
      end
    end
    private_class_method :trap_signals
  end
end
