module RedisFailover
  class FailoverStrategy
    # Failover strategy that selects an availaboe node that is both seen by all
    # node managers and has the lowest reported health check latency.
    class Latency < FailoverStrategy
      # @see RedisFailover::FailoverStrategy#find_candidate
      def find_candidate(snapshots)
        logger.info('Attempting to find candidate from snapshots:')
        logger.info("\n" + snapshots.values.join("\n"))
        all_node_managers = Set.new
        all_node_managers.merge(snapshots.values.map(&:node_managers).flatten)
        candidates = {}
        snapshots.each do |node, snapshot|
          if snapshot.available_count == all_node_managers.size
            candidates[node] = snapshot.avg_latency
          end
        end

        if candidate = candidates.min_by(&:last)
          candidate.first
        end
      end
    end
  end
end
