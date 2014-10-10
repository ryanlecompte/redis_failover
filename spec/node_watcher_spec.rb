require 'spec_helper'

module RedisFailover
  class LightNodeManager
    def initialize
      @node_states = {}
    end

    def notify_state(node, lag, latency)
      @node_states[node] = {lag: lag, latency: latency}
    end

    def state_for(node)
      NodeSnapshot.new('test').state_from_report(@node_states[node])
    end

    def electable?(node)
      snapshot = NodeSnapshot.new('test')
      snapshot.update_state_from_report(node, @node_states[node])
      snapshot.electable_nodes.include?(node)
    end
  end

  describe NodeWatcher do
    let(:node_manager) { LightNodeManager.new }
    let(:node) do
      node =  Node.new(:host => 'host', :port => 123).extend(RedisStubSupport)
      node.redis.slaveof('a', 'master')
      node
    end

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
          node_manager.notify_state(node, -1, -1)
          watcher = NodeWatcher.new(node_manager, node, 1)
          watcher.watch
          sleep(3)
          watcher.shutdown
          node_manager.state_for(node).should == :available
        end

        it 'properly informs manager node is electable when node is out of sync' do
          node_manager.notify_state(node, 0, 0)
          node.redis.slave_out_of_sync(false)
          watcher = NodeWatcher.new(node_manager, node, 1)
          watcher.watch
          sleep(3)
          watcher.shutdown
          node_manager.state_for(node).should == :unavailable
          node_manager.electable?(node).should == true
        end
      end

      context 'node is syncing with master' do
        it 'properly informs manager node is up when serve-stale-data is true' do
          node_manager.notify_state(node, -1, -1)
          node.redis.force_sync_with_master(true)
          watcher = NodeWatcher.new(node_manager, node, 1)
          watcher.watch
          sleep(3)
          watcher.shutdown
          node_manager.state_for(node).should == :available
        end

        it 'properly informs manager node is down when serve-stale-data is false' do
          node_manager.notify_state(node, 0, 0)
          node.redis.force_sync_with_master(false)
          watcher = NodeWatcher.new(node_manager, node, 1)
          watcher.watch
          sleep(3)
          watcher.shutdown
          node_manager.state_for(node).should == :unavailable
        end
      end
    end
  end
end
