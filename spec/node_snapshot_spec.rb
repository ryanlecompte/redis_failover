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
        snapshot.update_state('nm1', 0, 0)
        snapshot.update_state('nm2', 0, 0)
        snapshot.available_count.should == 2
      end
    end

    describe '#unviewable_by' do
      it 'updates the unavailability count' do
        snapshot.update_state('nm1', -1, -1)
        snapshot.update_state('nm2', -1, -1)
        snapshot.unavailable_count.should == 2
      end
    end
  end
end
