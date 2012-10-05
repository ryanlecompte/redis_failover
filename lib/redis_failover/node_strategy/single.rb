module RedisFailover
  class NodeStrategy
    # Marks the node as unavailable if any node manager reports the node as down.
    class Single < NodeStrategy
      # @see RedisFailover::NodeStrategy#determine_state
      def determine_state(node, snapshots)
        snapshot = snapshots[node]
        if snapshot.unavailable_count > 0
          log_unavailable(node, snapshot)
          :unavailable
        else
          :available
        end
      end
    end
  end
end
