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

    def new_zookeeper_client(servers)
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

      it 'does not rebuild clients if hosts have not changed' do
        class << client
          attr_reader :built_new_client
          def new_clients_for(*)
            @built_new_client = true
          end
        end

        5.times { client.send(:build_clients) }
        client.built_new_client.should_not be_true
      end
    end

    describe '#dispatch' do
      it 'routes write operations to master' do
        client.current_master.should_receive(:del)
        client.del('foo')
      end

      it 'routes read operations to a slave' do
        client.current_slaves.first.change_role_to('slave')
        client.current_slaves.first.should_receive(:get)
        client.get('foo')
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

      it 'fails hard when ZooKeeper is unavailable' do
        expect do
          Client.new(:zkservers => 'localhost:99991')
        end.to raise_error(ZookeeperError)
      end

      it 'properly detects when a node has changed roles' do
        client.current_master.change_role_to('slave')
        expect { client.send(:master) }.to raise_error(InvalidNodeRoleError)
      end

      it 'raises error for unsupported operations' do
        expect { client.select }.to raise_error(UnsupportedOperationError)
      end
    end
  end
end
