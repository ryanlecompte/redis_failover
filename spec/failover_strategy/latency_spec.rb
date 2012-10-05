require 'spec_helper'

module RedisFailover
  module FailoverStrategy
    FailoverStrategy.for(:latency)

    describe Latency do
      let(:snapshot) { NodeSnapshot.new(Node.new(:host => 'localhost', :port => '123')) }

      describe '#find_candidate' do
        it 'returns only candidates seen by all node managers' do
          strategy = FailoverStrategy.for(:latency)
          snapshot_1 = NodeSnapshot.new(Node.new(:host => 'localhost', :port => '123'))
          snapshot_1.viewable_by('nm1', 0)
          snapshot_1.unviewable_by('nm2')

          snapshot_2 = NodeSnapshot.new(Node.new(:host => 'localhost', :port => '456'))
          snapshot_2.viewable_by('nm2', 0)
          snapshot_2.unviewable_by('nm1')

          strategy.find_candidate([snapshot_1, snapshot_2]).should be_nil
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

          strategy.find_candidate([snapshot_1, snapshot_2]).should == snapshot_2.node
        end
      end
    end
  end
end
