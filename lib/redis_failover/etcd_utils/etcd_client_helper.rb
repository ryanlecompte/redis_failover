require 'digest'

module RedisFailover
  module EtcdClientHelper
    def self.included(base)
      base.class_exec do
        @initialize_overide = true
        def initialize_overide(*args)
          @threads = []
          initialize_old(*args)
        end

        def self.method_added(method_name)
          if method_name == :initialize && @initialize_overide
            @initialize_overide = false
            alias_method :initialize_old, :initialize
            alias_method :initialize, :initialize_overide
          end
        end
      end
    end

    def watch_etcd_folder(path, recursive = true, tries = 0)
      @threads << Thread.new do
        loop do
          begin
            name = Digest::MD5.hexdigest(path)
            watch_options = {recursive: recursive, waitIndex: instance_variable_get("@index_#{name}")}
            response = Timeout::timeout(@etcd.read_timeout) {@etcd.watch(path,watch_options)}
            instance_variable_set("@index_#{name}", response.etcd_index)

            yield(response) if block_given?
          rescue Timeout::Error
            retry
          rescue Etcd::EventIndexCleared
            instance_variable_set("@index_#{name}", nil)
            retry
          rescue
            (tries += 1) <= 3 ? retry : raise
          end
        end
      end
    end

    # Configures the Etcd client.
    def configure_etcd
      retries = 0

      begin
        @etcd = @etcd_nodes.detect do |etcd_client|
          etcd_client.machines.first["#{etcd_client.host}:#{etcd_client.port}"] rescue nil
        end

        raise EtcdNoMasterError, "Can't detect master in #{@etcd_nodes}" unless @etcd
      rescue
        sleep 1
        retry if (retries += 1) <= 3
        raise
      end
    end

    def wait_threads_completion
      @threads.each(&:join)
    end

    def terminte_threads
      @threads.each(&:terminate)
    end
  end
end