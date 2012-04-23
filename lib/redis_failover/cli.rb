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

        opts.on '-C', '--config PATH', "Path to YAML configuration file" do |file|
          options[:config_file] = file
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

      if config_file = options[:config_file]
        options = from_file(config_file)
      end

      prepare(options)
    end

    def self.required_options_missing?(options)
      return false if options[:config_file]
      return true if options.empty?
      return true unless options.values_at(:nodes, :zkservers).all?
      false
    end

    def self.from_file(file)
      unless File.exists?(file)
        raise ArgumentError, "File #{file} can't be found"
      end
      options = YAML.load_file(file)
      options[:nodes] = options[:nodes].join(',')
      options[:zkservers] = options[:zkservers].join(',')

      options
    end

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

      options
    end
  end
end