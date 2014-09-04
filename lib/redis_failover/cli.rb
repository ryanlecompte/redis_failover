module RedisFailover
  # Parses server command-line arguments.
  class CLI
    # Parses the source of options.
    #
    # @param [Array] source the command-line options to parse
    # @return [Hash] the parsed options
    def self.parse(source)
      options = {}
      parser = OptionParser.new do |opts|
        opts.banner = "Usage: redis_node_manager [OPTIONS]"
        opts.separator ""
        opts.separator "Specific options:"

        opts.on('-n', '--nodes NODES',
          'Comma-separated redis host:port pairs') do |nodes|
         options[:nodes] = nodes
        end

        opts.on('-z', '--zkservers SERVERS',
          'Comma-separated ZooKeeper host:port pairs') do |servers|
          options[:zkservers] = servers
        end

        opts.on('-p', '--password PASSWORD', 'Redis password') do |password|
          options[:password] = password
        end

        opts.on('--znode-path PATH',
          'Znode path override for storing redis server list') do |path|
          options[:znode_path] = path
        end

        opts.on('--max-failures COUNT',
          'Max failures before manager marks node unavailable') do |max|
          options[:max_failures] = Integer(max)
        end

        opts.on('-C', '--config PATH', 'Path to YAML config file') do |file|
          options[:config_file] = file
        end

        opts.on('--with-chroot ROOT', 'Path to ZooKeepers chroot') do |chroot|
          options[:chroot] = chroot
        end

        opts.on('-E', '--environment ENV', 'Config environment to use') do |config_env|
          options[:config_environment] = config_env
        end

        opts.on('--node-strategy STRATEGY',
         'Strategy used when determining availability of nodes (default: majority)') do |strategy|
          options[:node_strategy] = strategy
        end

        opts.on('--failover-strategy STRATEGY',
         'Strategy used when failing over to a new node (default: latency)') do |strategy|
          options[:failover_strategy] = strategy
        end

        opts.on('--required-node-managers COUNT',
         'Required Node Managers that must be reachable to determine node state (default: 1)') do |count|
          options[:required_node_managers] = Integer(count)
        end

        opts.on('-h', '--help', 'Display all options') do
          puts opts
          exit
        end
      end

      parser.parse(source)
      if config_file = options[:config_file]
        options = from_file(config_file, options[:config_environment])
      end

      if invalid_options?(options)
        puts parser
        exit
      end

      prepare(options)
    end

    # @return [Boolean] true if required options missing, false otherwise
    def self.invalid_options?(options)
      return true if options.empty?
      return true if options[:nodes].nil? || options.values_at(:etcd_nodes, :zkservers).all?(&:nil?)
      false
    end

    # Parses options from a YAML file.
    #
    # @param [String] file the filename
    # @params [String] env the environment
    # @return [Hash] the parsed options
    def self.from_file(file, env = nil)
      unless File.exists?(file)
        raise ArgumentError, "File #{file} can't be found"
      end
      options = YAML.load_file(file)

      if env
        options = options.fetch(env.to_sym) do
          raise ArgumentError, "Environment #{env} can't be found in config"
        end
      end

      options[:nodes] = options[:nodes].join(',')
      options[:zkservers] = options[:zkservers] && options[:zkservers].join(',')
      options[:etcd_nodes] = options[:etcd_nodes] && options[:etcd_nodes].map{|n| Util.symbolize_keys(n)}

      options
    end

    # Prepares the options for the rest of the system.
    #
    # @param [Hash] options the options to prepare
    # @return [Hash] the prepared options
    def self.prepare(options)
      options.each_value { |v| v.strip! if v.respond_to?(:strip!) }
      # turns 'host1:port,host2:port' => [{:host => host, :port => port}, ...]
      options[:nodes] = options[:nodes].split(',').map do |node|
        Hash[[:host, :port].zip(node.split(':'))]
      end

      # assume password is same for all redis nodes
      if password = options[:password]
        options[:nodes].each { |opts| opts.update(:password => password) }
      end

      if node_strategy = options[:node_strategy]
        options[:node_strategy] = node_strategy.to_sym
      end

      if failover_strategy = options[:failover_strategy]
        options[:failover_strategy] = failover_strategy.to_sym
      end

      options
    end
  end
end
