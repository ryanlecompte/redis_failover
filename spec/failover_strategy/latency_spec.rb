require 'spec_helper'

module RedisFailover
  class FailoverStrategy
    FailoverStrategy.for(:latency)

    describe Latency do
      describe '#find_candidate' do
        it 'returns only candidates seen by all node managers' do
          strategy = FailoverStrategy.for(:latency)
          snapshot_1 = NodeSnapshot.new(Node.new(:host => 'localhost', :port => '123'))
          snapshot_1.viewable_by('nm1', 0)
          snapshot_1.unviewable_by('nm2')

          snapshot_2 = NodeSnapshot.new(Node.new(:host => 'localhost', :port => '456'))
          snapshot_2.viewable_by('nm2', 0)
          snapshot_2.unviewable_by('nm1')

          snapshots = {snapshot_1.node => snapshot_1, snapshot_2.node => snapshot_2}
          strategy.find_candidate(snapshots).should be_nil
        end

        it 'returns the candidate with the lowest average latency' do
          strategy = FailoverStrategy.for(:latency)
          snapshot_1 = NodeSnapshot.new(Node.new(:host => 'localhost', :port => '123'))
          snapshot_1.viewable_by('nm1', 5)
          snapshot_1.viewable_by('nm2', 4)
          snapshot_1.viewable_by('nm3', 3)

          snapshot_2 = NodeSnapshot.new(Node.new(:host => 'localhost', :port => '456'))
          snapshot_2.viewable_by('nm1', 1)
          snapshot_2.viewable_by('nm2', 1)
          snapshot_2.viewable_by('nm3', 2)

          snapshots = {snapshot_1.node => snapshot_1, snapshot_2.node => snapshot_2}
          strategy.find_candidate(snapshots).should == snapshot_2.node
        end
      end
    end
  end
end
