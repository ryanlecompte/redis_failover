module RedisFailover
  # Parses server command-line arguments.
  class CLI
    def self.parse(source)
      return {} if source.empty?

      options = {}
      parser = OptionParser.new do |opts|
        opts.banner = "Usage: redis_failover_server [OPTIONS]"

        opts.on('-p', '--port port', 'Server port') do |port|
          options[:port] = Integer(port)
        end

        opts.on('-n', '--nodes nodes', 'Comma-separated list of redis host:port pairs') do |nodes|
          # turns 'host1:port,host2:port' => [{:host => host, :port => port}, ...]
          options[:nodes] = nodes.split(',').map { |node| Hash[[:host, :port].zip(node.strip.split(':'))] }
        end

        opts.on('-h', '--help', 'Display all options') do
          puts opts
          exit
        end
      end

      parser.parse!
      options
    end
  end
end