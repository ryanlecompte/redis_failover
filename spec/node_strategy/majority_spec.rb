require 'spec_helper'

module RedisFailover
  module NodeStrategy
    NodeStrategy.for(:majority)

    describe Majority do
      let(:snapshot) { NodeSnapshot.new(Node.new(:host => 'localhost', :port => '123')) }

      describe '#determine_state' do
        let(:node) { Node.new(:host => 'localhost', :port => '123') }

        it 'returns the unavailable state if unavailable by the majority of node managers' do
          strategy = NodeStrategy.for(:majority)
          snapshot.viewable_by('nm1')
          snapshot.unviewable_by('nm2')
          snapshot.unviewable_by('nm3')
          strategy.determine_state(snapshot).should == :unavailable
        end
      end
    end
  end
end
