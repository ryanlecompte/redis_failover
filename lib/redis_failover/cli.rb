module RedisFailover
  # Parses server command-line arguments.
  class CLI
    def self.parse(source)
      return {} if source.empty?

      options = {}
      parser = OptionParser.new do |opts|
        opts.banner = "Usage: redis_node_manager [OPTIONS]"

        opts.on('-p', '--password password', 'Redis password (optional)') do |password|
          options[:password] = password.strip
        end

        opts.on('-n', '--nodes redis nodes', 'Comma-separated redis host:port pairs (required)') do |nodes|
          # turns 'host1:port,host2:port' => [{:host => host, :port => port}, ...]
          options[:nodes] = nodes.split(',').map do |node|
            Hash[[:host, :port].zip(node.strip.split(':'))]
          end
        end

        opts.on('-z', '--zkservers zookeeper servers', 'Comma-separated zookeeper host:port pairs (required)') do |servers|
          options[:zkservers] = servers
        end

        opts.on('--znode-path path',
          'Znode path override for storing redis server list (optional)') do |path|
          options[:znode_path] = path
        end

        opts.on('--max-failures count',
          'Max failures before manager marks node unavailable (default 3)') do |max|
          options[:max_failures] = Integer(max)
        end

        opts.on('-h', '--help', 'Display all options') do
          puts opts
          exit
        end
      end

      parser.parse(source)
      # assume password is same for all redis nodes
      if password = options[:password]
        options[:nodes].each { |opts| opts.update(:password => password) }
      end

      options
    end
  end
end