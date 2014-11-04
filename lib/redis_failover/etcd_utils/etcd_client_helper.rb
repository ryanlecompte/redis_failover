require 'digest'

module RedisFailover
  module EtcdClientHelper
    def self.included(base)
      base.class_exec do
        @@initialize_override = true
        def initialize_override(*args)
          @threads = []
          @etcd_connection_lock = Monitor.new
          initialize_old(*args)
        end

        def self.method_added(method_name)
          if method_name == :initialize && @@initialize_override
            @@initialize_override = false
            alias_method :initialize_old, :initialize
            alias_method :initialize, :initialize_override
            @@initialize_override = true
          end
        end
      end
    end

    def etcd
      @etcd_connection_lock.synchronize do
        if @pid.nil?
          etcd_connect
        elsif @pid != Process.pid || @etcd.nil?
          etcd_reconnect
        end
      end

      return @etcd
    end

    def etcd_connect
      @lock = Monitor.new
      @pid = Process.pid
      @threads = []
    end

    # Still needs to reinitialize @pid, mutexes and etcd client
    def etcd_reconnect
      logger.info("Reconnect triggered. Reconnecting client in progress...")
      terminate_threads
      etcd_connect
    end

    def watch_etcd_folder(path, recursive = true, &block)
      @threads << Thread.new do
        loop do
          begin
            wait_for_change(path, recursive, 0, &block)
          rescue Errno::ECONNREFUSED
            logger.error("Failed to watch the current folder: #{path}")
            break
          end
        end
      end
    end

    def wait_for_change(path, recursive, tries, &block)
      begin
        name = Digest::MD5.hexdigest(path)
        watch_options = {recursive: recursive, waitIndex: instance_variable_get("@index_#{name}")}
        response = Timeout::timeout(@etcd.read_timeout) {@etcd.watch(path,watch_options)}
        instance_variable_set("@index_#{name}", response.node.modified_index + 1)

        yield(response) if block_given?
      rescue Timeout::Error
        retry
      rescue Etcd::EventIndexCleared
        instance_variable_set("@index_#{name}", nil)
        retry
      rescue => ex
        logger.error("Error while trying to watch for changes at #{path}. Error #{ex.class} => #{ex.message}")
        sleep 1
        (tries += 1) <= 3 ? retry : raise
      end
    end

    # Configures the Etcd client.
    def configure_etcd
      retries = 0

      begin
        @etcd_nodes_options.each do |etcd_options|
          etcd_client = Etcd.client(etcd_options)
          leader = etcd_client.machines.first["#{etcd_client.host}:#{etcd_client.port}"] rescue nil
          break @etcd = etcd_client if leader
        end

        raise EtcdNoMasterError, "Can't detect master in #{@etcd_nodes_options}" unless @etcd
      rescue
        sleep 1
        retry if (retries += 1) <= 3
        raise
      end
    end

    def wait_threads_completion
      @threads.each(&:join)
    end

    def terminate_threads
      @threads.each(&:terminate)
    end
  end
end