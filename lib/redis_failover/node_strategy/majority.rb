module RedisFailover
  class NodeStrategy
    # Majority strategy only marks the node as unavailable if a majority of the
    # snapshot indicates that the node is down.
    class Majority < NodeStrategy
      # @see RedisFailover::NodeStrategy#determine_state
      def determine_state(node, snapshots)
        snapshot = snapshots[node]
        if snapshot.unavailable_count > snapshot.available_count
          log_unavailable(node, snapshot)
          :unavailable
        else
          :available
        end
      end
    end
  end
end
