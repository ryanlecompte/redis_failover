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
        snapshot.viewable_by('nm1', 0)
        snapshot.viewable_by('nm2', 0)
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
  end
end
