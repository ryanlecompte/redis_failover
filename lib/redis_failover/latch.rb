module RedisFailover
  # Latch is a ZK group-based primitive that can be used to pause execution
  # until members present in an initial group have migrated to a new group.
  class Latch
    include Util

    # Creates a new Latch instance.
    #
    # @param [ZK::Client] zk the ZK client instance
    # @param [String] start_name name of start group
    # @param [String] end_name name of end group
    # @param [Integer] timeout the latch timeout in seconds
    def initialize(zk, start_name, end_name, timeout)
      @lock = Mutex.new
      @zk = zk
      @timeout = timeout
      @start_group = group_for(start_name)
      @end_group = group_for(end_name)
      @start_members = @start_group.member_names(:watch => true)
      logger.info("Created latch (#{self}) with start count: #{@start_members.size}")

      @start_group.on_membership_change do |old_members, current_members|
        @lock.synchronize do
          @start_members -= diff(@start_members, current_members)
        end
      end
    end

    # Waits until all members from start group appear in the end group, or a
    # configured timeout was reached.
    #
    # @return [Boolean] true if successful, false otherwise
    def await
      deadline = Time.now + @timeout
      wait_until(deadline) do
        @lock.synchronize do
          @end_group.member_names.size >= @start_members.size
        end
      end
    ensure
      stop_watching_groups
    end

    # @return [String] a textual representation of this latch
    def to_s
      "start_group = #{@start_group.name}, end_group = #{@end_group.name}"
    end

    private

    # @return [ZK::Group] a group for the specified group name
    def group_for(name)
      group = ZK::Group.new(@zk, name)
      group.create
      group
    end

    # Waits until the deadline or the condition evaluates to true.
    #
    # @param [Time] deadline the deadline time
    # @param [Proc] condition the condition at which to stop sleeping
    # @return [Boolean] true if condition met before deadline, false otherwise
    def wait_until(deadline, &condition)
      remaining = deadline - Time.now
      while remaining > 0
        return true if condition.call
        sleep([remaining, 1].min)
        remaining = deadline - Time.now
      end

      # timeout exceeded
      false
    end

    # Gracefully stop watching start/end groups.
    def stop_watching_groups
      [@start_group, @end_group].each do |group|
        begin
          group.close
        rescue => ex
          # best effort
          logger.warn("Failed to gracefully close group #{group.name}: #{ex.inspect}")
          logger.warn(ex.backtrace.join("\n"))
        end
      end
    end
  end
end
