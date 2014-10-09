require 'spec_helper'

module RedisFailover
  class NodeStrategy
    NodeStrategy.for(:consensus)

    describe Consensus do
      let(:node) { Node.new(:host => 'localhost', :port => '123') }
      let(:snapshot) { NodeSnapshot.new(node) }

      describe '#determine_state' do
        it 'returns the unavailable state if unavailable by all node managers' do
          strategy = NodeStrategy.for(:consensus)
          snapshot.update_state('nm1', -1, -1)
          snapshot.update_state('nm2', -1, -1)
          snapshot.update_state('nm3', -1, -1)
          strategy.determine_state(node, node => snapshot).should == :unavailable
        end

        it 'returns the available state if unavailable by some node managers' do
          strategy = NodeStrategy.for(:consensus)
          snapshot.update_state('nm1', -1, -1)
          snapshot.update_state('nm2', -1, -1)
          snapshot.update_state('nm3', 0, -1)
          strategy.determine_state(node, node => snapshot).should == :available
        end
      end
    end
  end
end
