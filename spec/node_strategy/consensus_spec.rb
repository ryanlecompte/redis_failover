require 'spec_helper'

module RedisFailover
  module NodeStrategy
    NodeStrategy.for(:consensus)

    describe Consensus do
      let(:snapshot) { NodeSnapshot.new(Node.new(:host => 'localhost', :port => '123')) }

      describe '#determine_state' do
        let(:node) { Node.new(:host => 'localhost', :port => '123') }

        it 'returns the unavailable state if unavailable by all node managers' do
          strategy = NodeStrategy.for(:consensus)
          snapshot.unviewable_by('nm1')
          snapshot.unviewable_by('nm2')
          snapshot.unviewable_by('nm3')
          strategy.determine_state(snapshot).should == :unavailable
        end

        it 'returns the available state if unavailable by some node managers' do
          strategy = NodeStrategy.for(:consensus)
          snapshot.unviewable_by('nm1')
          snapshot.unviewable_by('nm2')
          snapshot.viewable_by('nm3', 0)
          strategy.determine_state(snapshot).should == :available
        end
      end
    end
  end
end
