require 'socket'
require_relative 'node_manager_impl'
Dir["#{File.dirname(__FILE__)}/../etcd_utils/*.rb"].each {|file| require file }
module RedisFailover
  # NodeManager manages a list of redis nodes. Upon startup, the NodeManager
  # will discover the current redis master and slaves. Each redis node is
  # monitored by a NodeWatcher instance. The NodeWatchers periodically
  # report the current state of the redis node it's watching to the
  # NodeManager. The NodeManager processes the state reports and reacts
  # appropriately by handling stale/dead nodes, and promoting a new redis master
  # if it sees fit to do so.
  class EtcdNodeManager < NodeManagerImpl
    include RedisFailover::EtcdClientHelper

    def initialize(options)
      etcd_init
      @lock_key_timeout = options[:lock_key_timeout] || 10
      @lock_key_heartbeat = options[:lock_key_heartbeat] || 3
      @nb_retries = options[:retries] || 3
      @etcd_nodes_options = options[:etcd_nodes] || []
      super(options)
    end

    # Starts the node manager.
    #
    # @note This method does not return until the manager terminates.
    def start
      return unless running?
      begin
        setup_etcd
        spawn_watchers
        wait_until_master
      rescue *ETCD_ERRORS, SocketError => ex
        manage_start_error("ETCD error while attempting to manage nodes: #{ex.inspect}")
        retry
      rescue NoMasterError
        manage_start_error("Failed to promote a new master after #{MAX_PROMOTION_ATTEMPTS} attempts.")
        retry
      rescue => ex
        logger.error("Error Something went wrong: #{ex.class} \n #{ex.message} \n #{ex.backtrace.join("\n")}")
      ensure
        @monitor_state_thread.terminate if @monitor_state_thread
        @monitor_state_thread = nil
        wait_threads_completion
      end
    end

    # Performs a reset of the manager.
    def reset
      @lock.synchronize { @shutdown = true}
      @master_manager = false
      @master_promotion_attempts = 0

      terminate_threads
      @monitor_state_thread.terminate if @monitor_state_thread
      @watchers.each(&:shutdown) if @watchers

      @etcd_lock && @etcd_lock.unlock
      @etcd = @etcd_lock = @monitor_state_thread = nil
    end

    # Initiates a `hot` restart.
    def restart
      logger.info('Process restarting ...')
      reset
      @lock.synchronize { @shutdown = false}
      start
    end

    private

    # Configures the Etcd client.
    def setup_etcd
      unless @etcd
        safe_init_etcd
        etcd_listen_manual_failover
      end

      create_path(@root_node, dir: true)
      create_path(current_state_root, dir: true)
    end

    # Logs the error message and resets
    def manage_start_error(error_message)
      logger.error(error_message)
      reset
      @lock.synchronize { @shutdown = false}
      sleep(TIMEOUT)
    end

    # Listens for changes in the manual failover folder
    # Execute an action if there's any change
    def etcd_listen_manual_failover
      begin
        watch_etcd_folder(manual_failover_path) {|response| handle_manual_failover_update(response)}
      rescue => ex
        logger.error("Failed to listen to manual_failover_path: #{manual_failover_path}")
        logger.error(ex.backtrace.join("\n"))
      end
    end

    # Seeds the initial node master from an existing node config.
    def find_existing_master
      if data = handle_etcd_failures {@etcd.get(redis_nodes_path).value}
        nodes = symbolize_keys(decode(data))
        master = node_from(nodes[:master])
        logger.info("Master from existing node config: #{master || 'none'}")
        # Check for case where a node previously thought to be the master was somehow
        # manually reconfigured to be a slave outside of the node manager's control.
        begin
          if master && master.slave?
            raise InvalidNodeRoleError.new(master, :master, :slave)
          end
        rescue NodeUnavailableError => ex
          logger.warn("Failed to check whether existing node master [#{master}] has invalid role: #{ex.inspect}")
        end

        master
      end
    rescue Etcd::KeyNotFound
      # blank slate, no last known master
      nil
    end

    # Spawns the {RedisFailover::NodeWatcher} instances for each managed node.
    def spawn_watchers
      @etcd.delete(current_state_path) rescue nil # Best effort
      @monitored_state = {}

      @watchers = @nodes.map {|node| NodeWatcher.new(self, node, @options.fetch(:max_failures, 3))}
      @watchers.each(&:watch)
      logger.info("Monitoring redis nodes at #{stringify_nodes(@nodes)}")
    end

    # Deletes the node path containing the redis nodes.
    #
    # @param [String] path the node path to delete
    def delete_path(path)
      handle_etcd_failures do
        if @etcd.exists?(path)
          @etcd.delete(path, recursive: true)
          logger.info("Deleted ETCD node #{path}")
        end
      end
    end

    # Creates a node path.
    #
    # @param [String] path the node path to create
    # @param [Hash] options the options used to create the path
    # @option options [String] :initial_value an initial value for the node
    # @option options [Boolean] :ephemeral true if node is ephemeral, false otherwise
    def create_path(path, options = {})
      begin
        handle_etcd_failures {@etcd.set(path, options)} unless @etcd.exists?(path)
      rescue => ex
        logger.warn("Something went wrong when trying to create directory: #{ex.message}.") # Best try
      end
    end

    # Writes state to a particular node path.
    #
    # @param [String] path the node path that should be written to
    # @param [String] value the value to write to the node
    # @param [Hash] options the default options to be used when creating the node
    # @note the path will be created if it doesn't exist
    def write_state(path, value, options = {})
      handle_etcd_failures {@etcd.set(path, options.merge(:value => value))}
    end

    # Handles failures due to any issues related to the http etcd connections
    # Sleeps and retries in case of failure, should be enough to fix network glitch
    # If not then assumes something is wrong with the current Etcd leader
    # Drops the connection and finds who's the new leader, then restarts
    def handle_etcd_failures
      tries = 0

      begin
        return yield if block_given?
      rescue Timeout::Error, Errno::ETIMEDOUT
        retry
      rescue *ETCD_ERRORS, Errno::ECONNREFUSED, EOFError => ex
        logger.error { "Caught #{ex.class} '#{ex.message}' - retrying ..." }
        sleep(1)

        if (tries += 1) <= @nb_retries
          retry
        else
          raise if tries > @nb_retries + @etcd_nodes_options.size
          logger.error { "Oops, more than [#{@nb_retries}] retries: establishing fresh ETCD client" }
          restart
        end
      end
    end

    # Handles a manual failover node update.
    #
    # @param Etcd::Response
    def handle_manual_failover_update(response)
      begin
        perform_manual_failover if response.action == "set" || response.action == "create"
      rescue => ex
        logger.error("Error scheduling a manual failover: #{ex.inspect}")
        logger.error(ex.backtrace.join("\n"))
      end
    end

    # Fetches each currently running node manager's view of the
    # world in terms of which nodes they think are available/unavailable.
    #
    # @return [Hash<String, Array>] a hash of node manager to host states
    def fetch_node_manager_states
      begin
        etcd_nodes = @etcd.get(current_state_root, recursive: true).children
        states = etcd_nodes.each_with_object({}) do |etcd_node, states|
          child = etcd_node.key.gsub(current_state_root + '/', '')

          states[child] = deep_symbolize_keys(decode(etcd_node.value))
        end
      rescue => ex
        logger.error("Failed to fetch states for #{current_state_root}: #{ex.inspect}")
        states ||= {}
      end
    end

    # Manages the redis nodes by periodically processing snapshots.
    def manage_nodes
      # Re-discover nodes, since the state of the world may have been changed
      # by the time we've become the primary node manager.
      mode = discover_nodes

      # ensure that slaves are correctly pointing to this master
      # then sleep if the master has been guessed(to avoid using not up to date snapshot)
      redirect_slaves_to(@master)
      sleep(CHECK_INTERVAL) if mode == :guessed

      # Periodically update master config state.
      while running? && master_manager? && @etcd_lock
        begin
          master_election
          sleep(CHECK_INTERVAL)
        rescue *ETCD_ERRORS, Errno::ECONNREFUSED
          restart
        end
      end
    end

    # Master election algorithm:
    # If enough snapshots are available, finds who's the current master or elects one.
    # Updates the state of the world to Etcd
    def master_election
      @etcd_lock.assert!
      @lock.synchronize do
        snapshots = current_node_snapshots
        if ensure_sufficient_node_managers(snapshots)

          sorted_snaps = snapshots.keys.sort_by {|node| node == @master ? 0 : 1 }  # process master node state first
          orig_master = @master

          sorted_snaps.each do |node|
            next if @master != orig_master && node == @master   # skip processing of the just-promoted slave in this cycle
            update_master_state(node, snapshots)
          end

          # flush current master state
          write_current_redis_nodes

          # check if we've exhausted our attempts to promote a master
          unless @master
            @master_promotion_attempts += 1
            raise NoMasterError if @master_promotion_attempts > MAX_PROMOTION_ATTEMPTS
          end
        end
      end
    end

    # Executes a block wrapped in a etcd exclusive lock.
    def with_lock
      acquire_lock

      if running?
        @etcd_lock.assert!
        yield
      end ensure if @etcd_lock
        begin
          @etcd_lock.unlock
        rescue => ex
          logger.warn("Failed to release lock: #{ex.inspect}")
        end
      end
    end

    def acquire_lock
      @etcd_lock ||= EtcdClientLock::SimpleLocker.new(@etcd, @root_node, @options.merge(lock_value: manager_id))

      handle_etcd_failures do
        begin
          @etcd_lock.lock
        rescue
          running? ? raise : return # handle shutdown case
        end
      end
    end

    # Perform a manual failover to a redis node.
    def perform_manual_failover
      @lock.synchronize do
        return unless running? && @master_manager && @etcd_lock

        begin
          @etcd_lock.assert!
          new_master = @etcd.get(manual_failover_path).value
          return unless new_master && new_master.size > 0

          logger.info("Received manual failover request for: #{new_master}")
          logger.info("Current nodes: #{current_nodes.inspect}")
          snapshots = current_node_snapshots

          node = if new_master == ManualFailover::ANY_SLAVE
            failover_strategy_candidate(snapshots)
          else
            node_from(new_master)
          end

          if node
            handle_manual_failover(node, snapshots)
          else
            logger.error('Failed to perform manual failover, no candidate found.')
          end
        rescue => ex
          logger.error("Error handling manual failover: #{ex.inspect}")
          logger.error(ex.backtrace.join("\n"))
        end
      end
    end

    # Writes the current monitored list of redis nodes. This method is always
    # invoked by all running node managers.
    def write_current_monitored_state
      heartbeat_monitored_state if @monitor_state_thread.nil? && running?
    end

    def heartbeat_monitored_state
      @monitor_state_thread.terminate if @monitor_state_thread
      @monitor_state_thread = Thread.new do
        loop do
          monitored_state = encode(node_availability_state)
          retries = 0
          begin
            @etcd.set(current_state_path, value: monitored_state, ttl: @lock_key_timeout)
            sleep @lock_key_heartbeat
          rescue => ex
            if (retries += 1) <= 3
              sleep 0.5
              retry
            else
              logger.error("Failed to update current redis state to Etcd: #{ex.inspect}")
            end
          end
        end
      end
    end
  end
end
