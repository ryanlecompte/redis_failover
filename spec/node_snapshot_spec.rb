require 'spec_helper'

module RedisFailover
  describe NodeSnapshot do
    let(:snapshot) { NodeSnapshot.new(Node.new(:host => 'localhost', :port => '123'), {}) }

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

    describe '#state' do
      let(:node) { Node.new(:host => 'localhost', :port => '123') }
      context 'majority decision mode' do
        it 'returns the state as seen by the majority node managers' do
          snapshot = NodeSnapshot.new(node, :majority)
          snapshot.viewable_by('nm1')
          snapshot.viewable_by('nm2')
          snapshot.unviewable_by('nm3')
          snapshot.state.should == :available
        end
      end

      context 'consensus decision mode' do
        it 'returns the unavailable state when no consensus among node managers' do
          snapshot = NodeSnapshot.new(node, :consensus)
          snapshot.viewable_by('nm1')
          snapshot.viewable_by('nm2')
          snapshot.unviewable_by('nm3')
          snapshot.state.should == :unavailable
        end

        it 'returns the available state if all majors think it is available' do
          snapshot = NodeSnapshot.new(node, :consensus)
          snapshot.viewable_by('nm1')
          snapshot.viewable_by('nm2')
          snapshot.viewable_by('nm3')
          snapshot.state.should == :available
        end
      end
    end
  end
end
