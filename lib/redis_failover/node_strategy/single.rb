module RedisFailover
  module NodeStrategy
    # Marks the node as unavailable if any node manager reports the node as down.
    class Single
      # Returns the state determined by this strategy.
      #
      # @param [NodeSnapshot] snapshot the node snapshot
      # @return [Symbol] the status
      def determine_state(snapshot)
        if snapshot.unavailable_count > 0
          :unavailable
        else
          :available
        end
      end
    end
  end
end
