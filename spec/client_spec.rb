require 'spec_helper'

module RedisFailover
  Client::Redis = RedisStub
  class ClientStub < Client
    def current_master
      @master
    end

    def current_slaves
      @slaves
    end

    def fetch_nodes
      {
        :master => 'localhost:6379',
        :slaves => ['localhost:1111'],
        :unavailable => []
      }
    end

    def setup_zookeeper_client
      update_znode_timestamp
    end
  end

  describe Client do
    let(:client) { ClientStub.new(:zkservers => 'localhost:9281') }

    describe '#build_clients' do
      it 'properly parses master' do
        client.current_master.to_s.should == 'localhost:6379'
      end

      it 'properly parses slaves' do
        client.current_slaves.first.to_s.should == 'localhost:1111'
      end
    end

    describe '#dispatch' do
      it 'routes write operations to master' do
        called = false
        client.current_master.define_singleton_method(:del) do |*args|
          called = true
        end
        client.del('foo')
        called.should be_true
      end

      it 'routes read operations to a slave' do
        called = false
        client.current_slaves.first.change_role_to('slave')
        client.current_slaves.first.define_singleton_method(:get) do |*args|
          called = true
        end
        client.get('foo')
        called.should be_true
      end

      it 'reconnects when node is unavailable' do
        class << client
          attr_reader :reconnected
          def build_clients
            @reconnected = true
            super
          end

          def fetch_nodes
            @calls ||= 0
            {
              :master => "localhost:222#{@calls += 1}",
              :slaves => ['localhost:1111'],
              :unavailable => []
            }
          end
        end

        client.current_master.make_unavailable!
        client.del('foo')
        client.reconnected.should be_true
      end

      it 'properly detects when a node has changed roles' do
        client.current_master.change_role_to('slave')
        expect { client.send(:master) }.to raise_error(InvalidNodeRoleError)
      end

      it 'raises error for unsupported operations' do
        expect { client.select }.to raise_error(UnsupportedOperationError)
      end

      it 'attempts ZK reconnect when no communication from Node Manager within certain time window' do
        client.instance_variable_set(:@last_znode_timestamp, Time.at(0))
        client.should_receive(:reconnect_zk)
        client.del('foo')
      end
    end
  end
end
