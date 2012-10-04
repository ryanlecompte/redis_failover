module RedisFailover
  module Strategy
    # Consensus strategy only marks the node as unavailable if all members of the
    # snapshot indicate that the node is down.
    class Consensus
      # Returns the state determined by this strategy.
      #
      # @param [NodeSnapshot] snapshot the node snapshot
      # @return [Symbol] the status
      def determine_state(snapshot)
        if snapshot.all_unavailable?
          :unavailable
        else
          :available
        end
      end
    end
  end
end
