module RedisFailover
  module EtcdClientLock
    ROOT_LOCK_SUFFIX = "_etcd_locking"
    class LockHoldError < StandardError; end

    class SimpleLocker
      attr_reader :etcd, :root_lock_path

      def initialize(client, root_lock_node=nil, options = {})
        @etcd = client
        @root_lock_path = root_lock_node || ROOT_LOCK_PREFIX + path
        @locked = false
        @waiting = false
        @lock_path = nil
        @mutex = Monitor.new
        @nb_retries = options[:retries] || 3
        @lock_key_timeout = options[:lock_key_timeout] || 10
        @lock_key_heartbeat = options[:lock_key_heartbeat] || 3
      end


      # block caller until lock is aquired, then yield
      # @yield [lock] calls the block with the lock instance when acquired
      #
      #
      # @raise [LockWaitTimeoutError] if the :wait timeout is exceeded
      def with_lock
        lock
        yield self ensure unlock
      end

      # the basename of our lock path
      #
      #
      # @return [nil] if lock_path is not set
      # @return [String] last path component of our lock path
      def lock_basename
        synchronize { lock_path && File.basename(lock_path) }
      end

      # @private
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


      def acquirable?
        raise NotImplementedError
      end

      # @return [true] if the lock was held and this method has
      #   unlocked it successfully
      #
      # @return [false] if the lock was not held
      def unlock
        result = false
        @mutex.synchronize do
          if @locked
            logger.debug { "unlocking" }
            result = cleanup_lock_path!
            @locked = false
          end
        end

        return result
      end

      # Set a client lock on the path passed in parameters during the object initialization
      def lock
        return true if @mutex.synchronize { @locked }

        create_lock_path!()
        begin
          if block_until_lock!
            @mutex.synchronize { @locked = true }
          else
            false
          end
        ensure
          cleanup_lock_path! unless @locked
        end
      end


      # This is for users who wish to check that the assumption is correct
      # that they actually still hold the lock. (check for session interruption,
      # perhaps a lock is obtained in one method and handed to another)
      #
      #
      # @example
      #
      #   def process_jobs
      #     @lock.with_lock do
      #       @jobs.each do |j|
      #         @lock.assert!
      #         perform_job(j)
      #       end
      #     end
      #   end
      #
      #   def perform_job(j)
      #     puts "hah! he thinks we're workin!"
      #     sleep(60)
      #   end
      #
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
          etcd.get(root_lock_path, recursive: true, sorted: true).children.select do |ary|
            ary.dir != true
          end
        end

        def create_root_path!
          tries = @nb_retries
          begin
            etcd.set(@root_lock_path, dir: true)
          rescue Etcd::NotFile
            # already exists
          rescue => ex
            (tries +=1) <= 3 ? retry : logger.warn { "Can't create directory `#{@root_lock_path}`: #{ex.message}" }
          end
        end

        def create_lock_path!
          begin
            tries = @nb_retries
            @mutex.synchronize do
              unless lock_path_exists?
                @lock_path = etcd.create_in_order(root_lock_path, ttl: @lock_key_timeout).key
                start_breathing!
              end
            end

            logger.debug { "got lock path #{@lock_path}" }
            @lock_path
          rescue
            (tries +=1) <= 3 ? retry : raise
          end
        end

        def start_breathing!
          @breathing_thread.terminate if @breathing_thread
          @breathing_thread = Thread.new do
            loop do
              begin
                etcd.set(@lock_path, ttl: @lock_key_heartbeat)
                sleep @lock_key_heartbeat
              rescue Timeout::Error
                retry
              rescue
                (tries += 1) <= 3 ? retry : raise
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
            if @lock_path && etcd.exists?(@lock_path)
              etcd.delete(@lock_path)
              logger.debug { "removing lock path #{@lock_path}" }
              result = true
            end

            @lock_path = nil
          end

          return result
        end

        # Download all locking keys and returns the ones that have
        def blocking_locks
          lock_keys = get_lock_children
          lock_path ? lock_keys.select {|lock| index_from_path(lock) < lock_number} : lock_keys
        end

        # Performs the checks that (according to the recipe) mean that we hold the lock.
        def got_lock?
          lock_path and blocking_locks.empty?
        end

        def block_until_lock!
          loop do
            @waiting = true

            if got_lock?
              @waiting = false
              return true
            end

            etcd.watch(root_lock_path, recursive: true)
          end
        end
    end
  end
end