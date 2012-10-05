module RedisFailover
  class NodeStrategy
    # Marks the node as unavailable if any node manager reports the node as down.
    class Single < NodeStrategy
      # @see RedisFailover::NodeStrategy#determine_state
      def determine_state(node, snapshots)
        if snapshots[node].unavailable_count > 0
          :unavailable
        else
          :available
        end
      end
    end
  end
end
