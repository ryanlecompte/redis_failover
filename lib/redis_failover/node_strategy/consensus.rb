module RedisFailover
  class NodeStrategy
    # Consensus strategy only marks the node as unavailable if all members of the
    # snapshot indicate that the node is down.
    class Consensus < NodeStrategy
      # @see RedisFailover::NodeStrategy#determine_state
      def determine_state(node, snapshots)
        if snapshots[node].all_unavailable?
          :unavailable
        else
          :available
        end
      end
    end
  end
end
