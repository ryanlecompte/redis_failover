module RedisFailover
  module FailoverStrategy
    # Failover strategy that selects an availaboe node that is both seen by all
    # node managers and has the lowest reported health check latency.
    class Latency
      include Util

      # Returns a candidate node as determined by this strategy.
      #
      # @param [Hash<Node, NodeSnapshot>] snapshots the node snapshots
      # @return [Node] the candidate node or nil if one couldn't be found
      def find_candidate(snapshots)
        logger.info('Attempting to find candidate from snapshots:')
        logger.info(snapshots.join("\n"))

        all_node_managers = Set.new
        all_node_managers.merge(snapshots.map(&:node_managers).flatten)
        candidates = {}
        snapshots.each do |snapshot|
          if snapshot.available_count == all_node_managers.size
            candidates[snapshot.node] = snapshot.avg_latency
          end
        end

        if candidate = candidates.min_by(&:last)
          candidate.first
        end
      end
    end
  end
end
