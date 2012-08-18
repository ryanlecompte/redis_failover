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

        opts.on('--decision-mode MODE',
         'Decision mode used when monitoring nodes (majority or consensus)') do |mode|
          options[:decision_mode] = mode
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
      return true unless options.values_at(:nodes, :zkservers).all?
      if (mode = options[:decision_mode]) && !%w(majority consensus).include?(mode)
        return true
      end

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
      options[:zkservers] = options[:zkservers].join(',')

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

      if mode = options[:decision_mode]
        options[:decision_mode] = mode.to_sym
      end

      options
    end
  end
end
