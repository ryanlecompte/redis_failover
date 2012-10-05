require 'spec_helper'

module RedisFailover
  class NodeStrategy
    NodeStrategy.for(:single)

    describe Single do
      let(:node) { Node.new(:host => 'localhost', :port => '123') }
      let(:snapshot) { NodeSnapshot.new(node) }

      describe '#determine_state' do
        it 'returns the unavailable state if any node manager reports as down' do
          strategy = NodeStrategy.for(:single)
          snapshot.unviewable_by('nm1')
          snapshot.viewable_by('nm2', 0)
          snapshot.viewable_by('nm3', 0)
          strategy.determine_state(node, node => snapshot).should == :unavailable
        end
      end
    end
  end
end
