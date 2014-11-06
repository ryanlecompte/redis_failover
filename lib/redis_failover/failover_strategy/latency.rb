module RedisFailover
  class FailoverStrategy
    # Failover strategy that selects an available node that is both seen by all
    # node managers and has the lowest reported health check latency.
    class Latency < FailoverStrategy
      # @see RedisFailover::FailoverStrategy#find_candidate
      def find_candidate(snapshots)
        candidates = {}
        snapshots.each do |node, snapshot|
          candidates[node] = snapshot.avg_lag if snapshot.all_electable?
        end

        if candidate = candidates.min_by(&:last)
          candidate.first
        end
      end
    end
  end
end
