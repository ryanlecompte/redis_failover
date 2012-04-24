module RedisFailover
  # Runner is responsible for bootstrapping the redis Node Manager.
  class Runner
    def self.run(options)
      options = CLI.parse(options)
      @node_manager = NodeManager.new(options)
      trap_signals
      node_manager_thread = Thread.new { @node_manager.start }
      node_manager_thread.join
    end

    def self.trap_signals
      [:INT, :TERM].each do |signal|
        trap(signal) do
          Util.logger.info('Shutting down ...')
          @node_manager.shutdown
          exit(0)
        end
      end
    end
  end
end
