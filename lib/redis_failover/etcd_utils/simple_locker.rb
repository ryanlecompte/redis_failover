module RedisFailover
  module EtcdClientLock
    ROOT_LOCK_SUFFIX = "/etcd_locking"

    class SimpleLocker
      attr_reader :etcd, :root_lock_path, :lock_path

      def initialize(client, root_lock_node, options = {})
        @etcd = client
        @root_lock_path = root_lock_node + ROOT_LOCK_SUFFIX
        @locked = false
        @waiting = false
        @lock_path = nil
        @mutex = Monitor.new
        @nb_retries = options[:retries] || 3
        @lock_key_timeout = options[:lock_key_timeout] || 10
        @lock_key_heartbeat = options[:lock_key_heartbeat] || 3
      end

      # @return [Logger] the logger instance to use
      def self.logger
        @logger ||= begin
          logger = Logger.new(STDOUT)
          logger.level = Logger::INFO
          logger.formatter = proc do |severity, datetime, progname, msg|
            "#{datetime.utc} RedisFailover(ETCD locker) #{Process.pid} #{severity}: #{msg}\n"
          end
          logger
        end
      end

      def logger
        self.class.logger
      end

      # Sets a new logger to use.
      #
      # @param [Logger] logger a new logger to use
      def self.logger=(logger)
        @logger = logger
      end

      # block caller until lock is acquire, then yield
      # @yield [lock] calls the block with the lock instance when acquired
      #
      #
      # @raise [LockWaitTimeoutError] if the :wait timeout is exceeded
      def with_lock
        lock
        yield self ensure unlock
      end

      def with_timeout_retries(tries = 0)
        begin
          yield
        rescue Timeout::Error, Errno::ETIMEDOUT
          retry if (tries += 1) > 5
        end
      end

      # the basename of our lock path
      #
      #
      # @return [nil] if lock_path is not set
      # @return [String] last path component of our lock path
      def lock_basename
        synchronize { lock_path && File.basename(lock_path) }
      end

      def lock_number
        synchronize { lock_path && index_from_path(lock_path) }
      end

      # returns the current idea of whether or not the lock is hold, which does
      # not actually check the state on the server.
      #
      # use #assert! if you want to do a real server check
      #
      # @return [true] if the lock is hold
      # @return [false] if the lock is NOT hold
      def locked?
        synchronize { !!@locked }
      end

      def waiting?
        synchronize { !!@waiting }
      end

      # @return [true] if the lock was held and this method has
      #   unlocked it successfully
      #
      # @return [false] if the lock was not held
      def unlock
        result = false
        @mutex.synchronize do
          result = cleanup_lock_path!
          if @locked
            logger.debug("unlocking")
            @locked = false
          end
        end

        return result
      end

      # Set a client lock on the path passed in parameters during the object initialization
      def lock
        return true if @mutex.synchronize { @locked }

        create_lock_path!
        begin
          if block_until_lock!
            @mutex.synchronize { @locked = true }
          else
            false
          end
        rescue => ex
          logger.error("Error, can't lock: #{ex.message}, #{ex.backtrace.join('\n')}")
          raise
        ensure
          cleanup_lock_path! unless @locked
        end
      end

      # Make sure the lock is still hold
      def assert!
        @mutex.synchronize do
          raise LockHoldError, "have not obtained the lock yet"            unless locked?
          raise LockHoldError, "lock_path was #{lock_path.inspect}"        unless lock_path
          raise LockHoldError, "the lock path #{lock_path} did not exist!" unless etcd.exists?(lock_path)
          raise LockHoldError, "we do not actually hold the lock"          unless got_lock?
        end
      end

      def assert
        assert!
        true
      rescue LockHoldError
        false
      end

      private
        def synchronize
          @mutex.synchronize { yield }
        end

        def index_from_path(path)
          path[/.*\/(\d+)$/, 1].to_i
        end

        def get_lock_children
          with_timeout_retries do
            etcd.get(root_lock_path, recursive: true, sorted: true).children.select do |ary|
              ary.dir != true
            end
          end
        end

        def smallest_lock_path?
          with_timeout_retries do
            smallest_lock_path = etcd.get(root_lock_path, recursive: true).children.min_by do |node|
              next Float::INFINITY if node.dir == true || node.ttl.to_i < 1
              index_from_path(node.key)
            end

            smallest_lock_path.key == lock_path
          end
        end

        # Etcd 0.4.6 sort is not working as expected(lexical sort vs numerical)
        # This algorithm won't work with that version
        def fast_smallest_lock_path?
          etcd.get(root_lock_path, recursive: true, sorted: true).children.any? do |node|
            next if node.dir == true || node.ttl.to_i < 1
            current_number = index_from_path(node.key)

            break false if current_number < lock_number
            current_number == lock_number
          end
        end

        def create_root_path!
          tries = 0
          begin
            etcd.set(@root_lock_path, dir: true)
          rescue Etcd::NotFile
            # already exists
          rescue => ex
            (tries +=1) <= @nb_retries ? retry : logger.warn("Can't create directory `#{@root_lock_path}`: #{ex.message}")
          end
        end

        def create_lock_path!(tries = 0)
          unless lock_path_exists?
            begin
              @mutex.synchronize do
                response = etcd.create_in_order(root_lock_path, ttl: @lock_key_timeout)
                @lock_path = response.node.key
                @index = response.etcd_index
                start_breathing!
                logger.debug("got lock path at #{@lock_path}")
              end
            rescue
              (tries += 1) <= @nb_retries ? retry : raise
            end
          end

          return @lock_path
        end

        def start_breathing!
          @breathing_thread.terminate if @breathing_thread
          @breathing_thread = Thread.new do
            loop do
              tries = 0

              begin
                etcd.set(@lock_path, ttl: @lock_key_timeout)
                sleep @lock_key_heartbeat
              rescue Timeout::Error, Errno::ETIMEDOUT
                retry
              rescue => ex
                if (tries += 1) <= @nb_retries
                  sleep 1
                  retry
                else
                  logger.error("Can't send heartbeat: #{ex.message}, #{ex.backtrace.join('\n')}")
                end
              end
            end
          end
        end

        # if we previously had a lock path, check if it still exists
        def lock_path_exists?
          @mutex.synchronize do
            return false unless @lock_path
            etcd.exists?(@lock_path)
          end
        end

        # we make a best-effort to clean up, this case is rife with race
        # conditions if there is a lot of contention for the locks, so if we
        # can't remove a path or if that path happens to not be empty we figure
        # either we got pwned or that someone else will run this same method
        # later and get to it
        #
        def cleanup_lock_path!
          result = false

          @mutex.synchronize do
            @breathing_thread.terminate if @breathing_thread
            if @lock_path
              begin
                etcd.delete(@lock_path)
                logger.debug("Removed lock path `#{@lock_path}`")
                result = true
              rescue Etcd::KeyNotFound
                logger.debug("Lock path `#{@lock_path}` not found")
              rescue => ex
                logger.error("Error When cleaning the lock path: #{ex.class}: #{ex.message}.")
              end
            end

            @lock_path = nil
          end

          return result
        end

        # Download all locking keys and returns the ones that have
        def blocking_locks
          lock_nodes = get_lock_children
          lock_path ? lock_nodes.select {|lock_node| index_from_path(lock_node.key) < lock_number} : lock_nodes
        end

        # Performs the checks that (according to the recipe) mean that we hold the lock.
        def got_lock?
          lock_path && smallest_lock_path?
        end

        def block_until_lock!
          loop do
            @waiting = true

            if got_lock?
              @waiting = false
              return true
            end

            begin
              @mutex.synchronize do
                watch_options = {recursive: true, waitIndex: @index}
                response = Timeout::timeout(etcd.read_timeout) {etcd.watch(root_lock_path, watch_options)}
                @index = response.node.modified_index + 1
              end
            rescue Timeout::Error
              @mutex.synchronize {@index = nil}
              retry
            rescue Etcd::EventIndexCleared
              @index = nil
              retry
            end
          end
        end
    end
  end
end