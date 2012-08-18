require 'spec_helper'

module RedisFailover
  describe NodeSnapshot do
    let(:snapshot) { NodeSnapshot.new(Node.new(:host => 'localhost', :port => '123')) }

    describe '#initialize' do
      it 'creates a new empty snapshot' do
        snapshot.available_count.should == 0
        snapshot.unavailable_count.should == 0
      end
    end

    describe '#viewable_by' do
      it 'updates the availability count' do
        snapshot.viewable_by('nm1')
        snapshot.viewable_by('nm2')
        snapshot.available_count.should == 2
      end
    end

    describe '#unviewable_by' do
      it 'updates the unavailability count' do
        snapshot.unviewable_by('nm1')
        snapshot.unviewable_by('nm2')
        snapshot.unavailable_count.should == 2
      end
    end

    describe '#majority_state' do
      it 'returns the state as seen by the majority node managers' do
        snapshot.viewable_by('nm1')
        snapshot.viewable_by('nm2')
        snapshot.unviewable_by('nm3')
        snapshot.majority_state.should == :available
      end
    end
  end
end
