module RedisFailover
  # Parses server command-line arguments.
  class CLI
    def self.parse(source)
      options = {}
      parser = OptionParser.new do |opts|
        opts.banner = "Usage: redis_node_manager [OPTIONS]"
        opts.separator ""
        opts.separator "Specific options:"

        opts.on('-n', '--nodes NODES',
          'Comma-separated redis host:port pairs') do |nodes|
          # turns 'host1:port,host2:port' => [{:host => host, :port => port}, ...]
          options[:nodes] = nodes.split(',').map do |node|
            Hash[[:host, :port].zip(node.strip.split(':'))]
          end
        end

        opts.on('-z', '--zkservers SERVERS',
          'Comma-separated ZooKeeper host:port pairs') do |servers|
          options[:zkservers] = servers.strip
        end

        opts.on('-p', '--password [PASSWORD]', 'Redis password') do |password|
          options[:password] = password.strip
        end

        opts.on('--znode-path [PATH]',
          'Znode path override for storing redis server list') do |path|
          options[:znode_path] = path
        end

        opts.on('--max-failures [COUNT]',
          'Max failures before manager marks node unavailable') do |max|
          options[:max_failures] = Integer(max)
        end

        opts.on('-h', '--help', 'Display all options') do
          puts opts
          exit
        end
      end

      parser.parse(source)
      if required_options_missing?(options)
        puts parser
        exit
      end

      # assume password is same for all redis nodes
      if password = options[:password]
        options[:nodes].each { |opts| opts.update(:password => password) }
      end

      options
    end

    def self.required_options_missing?(options)
      return true if options.empty?
      return true unless options.values_at(:nodes, :zkservers).all?
      false
    end
  end
end