require 'spec_helper'

module RedisFailover
  class LightNodeManager
    def initialize
      @node_states = {}
    end

    def notify_state(node, state, latency = nil)
      @node_states[node] = state
    end

    def state_for(node)
      @node_states[node]
    end
  end

  describe NodeWatcher do
    let(:node_manager) { LightNodeManager.new }
    let(:node) { Node.new(:host => 'host', :port => 123).extend(RedisStubSupport) }

    describe '#watch' do
      context 'node is not syncing with master' do
        it 'properly informs manager of unavailable node' do
          watcher = NodeWatcher.new(node_manager, node, 1)
          watcher.watch
          sleep(3)
          node.redis.make_unavailable!
          sleep(3)
          watcher.shutdown
          node_manager.state_for(node).should == :unavailable
        end

        it 'properly informs manager of available node' do
          node_manager.notify_state(node, :unavailable)
          watcher = NodeWatcher.new(node_manager, node, 1)
          watcher.watch
          sleep(3)
          watcher.shutdown
          node_manager.state_for(node).should == :available
        end
      end

      context 'node is syncing with master' do
        it 'properly informs manager of syncing node' do
          node_manager.notify_state(node, :unavailable)
          node.redis.slaveof('masterhost', 9876)
          node.redis.force_sync_with_master(true)
          watcher = NodeWatcher.new(node_manager, node, 1)
          watcher.watch
          sleep(3)
          watcher.shutdown
          node_manager.state_for(node).should == :available
        end
      end
    end
  end
end
