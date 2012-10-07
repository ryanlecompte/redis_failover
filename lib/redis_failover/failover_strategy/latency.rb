module RedisFailover
  class FailoverStrategy
    # Failover strategy that selects an available node that is both seen by all
    # node managers and has the lowest reported health check latency.
    class Latency < FailoverStrategy
      # @see RedisFailover::FailoverStrategy#find_candidate
      def find_candidate(snapshots)
        logger.info('Attempting to find candidate from snapshots:')
        logger.info("\n" + snapshots.values.join("\n"))

        candidates = {}
        snapshots.each do |node, snapshot|
          if snapshot.all_available?
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
