require 'spec_helper'

module RedisFailover
  module Strategy
    Strategy.for(:single)

    describe Single do
      let(:snapshot) { NodeSnapshot.new(Node.new(:host => 'localhost', :port => '123')) }

      describe '#determine_state' do
        let(:node) { Node.new(:host => 'localhost', :port => '123') }

        it 'returns the unavailable state if any node manager reports as down' do
          strategy = Strategy.for(:single)
          snapshot.unviewable_by('nm1')
          snapshot.viewable_by('nm2')
          snapshot.viewable_by('nm3')
          strategy.determine_state(snapshot).should == :unavailable
        end
      end
    end
  end
end
