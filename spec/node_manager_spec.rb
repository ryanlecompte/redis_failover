require 'spec_helper'

module RedisFailover
  describe NodeManager do
    let(:manager) { NodeManagerStub.new([]) }

    describe '#nodes' do
      it 'returns current master and slave nodes' do
        manager.nodes.should == {
          :master => 'master:6379',
          :slaves => ['slave:6379'],
          :unreachable => []
        }
      end
    end

    describe '#handle_unreachable' do
      context 'slave dies' do
        it 'moves slave to unreachable list' do
          slave = manager.slaves.first
          manager.force_unreachable(slave)
          manager.nodes[:unreachable].should include(slave.to_s)
        end
      end

      context 'master dies' do
        before(:each) do
          @slave = manager.slaves.first
          @master = manager.master
          manager.force_unreachable(@master)
        end

        it 'promotes slave to master' do
          manager.master.should == @slave
        end

        it 'moves master to unreachable list' do
          manager.nodes[:unreachable].should include(@master.to_s)
        end
      end
    end

    describe '#handle_reachable' do
      before(:each) do
        # force to be unreachable first
        @slave = manager.slaves.first
        manager.force_unreachable(@slave)
      end

      context 'slave node with a master present' do
        it 'removes slave from unreachable list' do
          manager.force_reachable(@slave)
          manager.nodes[:unreachable].should be_empty
          manager.nodes[:slaves].should include(@slave.to_s)
        end
      end

      context 'slave node with no master present' do
        before(:each) do
          @master = manager.master
          manager.force_unreachable(@master)
        end

        it 'promotes slave to master' do
          manager.master.should be_nil
          manager.force_reachable(@slave)
          manager.master.should == @slave
        end

        it 'slaves list remains empty' do
          manager.nodes[:slaves].should be_empty
        end
      end
    end
  end
end
