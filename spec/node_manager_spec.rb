require 'spec_helper'

module RedisFailover
  describe NodeManager do
    let(:manager) { NodeManagerStub.new({}) }

    describe '#nodes' do
      it 'returns current master and slave nodes' do
        manager.nodes.should == {
          :master => 'master:6379',
          :slaves => ['slave:6379'],
          :unavailable => []
        }
      end
    end

    describe '#handle_unavailable' do
      context 'slave dies' do
        it 'moves slave to unavailable list' do
          slave = manager.slaves.first
          manager.force_unavailable(slave)
          manager.nodes[:unavailable].should include(slave.to_s)
        end
      end

      context 'master dies' do
        before(:each) do
          @slave = manager.slaves.first
          @master = manager.master
          manager.force_unavailable(@master)
        end

        it 'promotes slave to master' do
          manager.master.should == @slave
        end

        it 'moves master to unavailable list' do
          manager.nodes[:unavailable].should include(@master.to_s)
        end
      end
    end

    describe '#handle_available' do
      before(:each) do
        # force to be unavailable first
        @slave = manager.slaves.first
        manager.force_unavailable(@slave)
      end

      context 'slave node with a master present' do
        it 'removes slave from unavailable list' do
          manager.force_available(@slave)
          manager.nodes[:unavailable].should be_empty
          manager.nodes[:slaves].should include(@slave.to_s)
        end

        it 'makes node a slave of new master' do
          manager.master = Node.new(:host => 'foo', :port => '7892')
          manager.force_available(@slave)
          @slave.fetch_info.should == {
            :role => 'slave',
            :master_host => 'foo',
            :master_port => '7892'}
        end

        it 'does not invoke slaveof operation if master has not changed' do
          @slave.redis.should_not_receive(:slaveof)
          manager.force_available(@slave)
        end
      end

      context 'slave node with no master present' do
        before(:each) do
          @master = manager.master
          manager.force_unavailable(@master)
        end

        it 'promotes slave to master' do
          manager.master.should be_nil
          manager.force_available(@slave)
          manager.master.should == @slave
        end

        it 'slaves list remains empty' do
          manager.nodes[:slaves].should be_empty
        end
      end
    end

    describe '#handle_syncing' do
      context 'prohibits stale reads' do
        it 'adds node to unavailable list' do
          slave = manager.slaves.first
          manager.force_syncing(slave, false)
          manager.nodes[:unavailable].should include(slave.to_s)
        end
      end

      context 'allows stale reads' do
        it 'makes node available' do
          slave = manager.slaves.first
          manager.force_syncing(slave, true)
          manager.nodes[:unavailable].should_not include(slave.to_s)
          manager.nodes[:slaves].should include(slave.to_s)
        end
      end
    end
  end
end
