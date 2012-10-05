require 'spec_helper'

module RedisFailover
  class NodeStrategy
    NodeStrategy.for(:majority)

    describe Majority do
      let(:node) { Node.new(:host => 'localhost', :port => '123') }
      let(:snapshot) { NodeSnapshot.new(node) }

      describe '#determine_state' do
        it 'returns the unavailable state if unavailable by the majority of node managers' do
          strategy = NodeStrategy.for(:majority)
          snapshot.viewable_by('nm1', 0)
          snapshot.unviewable_by('nm2')
          snapshot.unviewable_by('nm3')
          strategy.determine_state(node, node => snapshot).should == :unavailable
        end
      end
    end
  end
end
