module RedisFailover
  module NodeStrategy
    # Majority strategy only marks the node as unavailable if a majority of the
    # snapshot indicates that the node is down.
    class Majority
      # Returns the state determined by this strategy.
      #
      # @param [NodeSnapshot] snapshot the node snapshot
      # @return [Symbol] the status
      def determine_state(snapshot)
        if snapshot.unavailable_count > snapshot.available_count
          :unavailable
        else
          :available
        end
      end
    end
  end
end
