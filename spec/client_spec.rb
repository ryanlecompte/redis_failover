require 'spec_helper'

module RedisFailover
  ClientImpl::Redis = RedisStub
  ClientImpl::Redis::Client = Redis::Client
  class ZkClientStub < ZookeeperClient
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

    def setup_zk
      @zk = NullObject.new
      update_node_timestamp
    end
  end

  class EtcdClientStub < EtcdClient
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

    def setup_zk
      @etcd = NullObject.new
      update_node_timestamp
    end
  end

  shared_examples "FailoverClient" do |client_type|
    describe Client do
      let(:opts) {
                  if client_type == ZkClientStub
                    {:zkservers => 'localhost:9281', :safe_mode => true}
                  else
                    {:etcd => {:host => 'localhost', :port => '4001', :verify_mode => true}}
                  end
                }
      let(:client) { client_type.new(opts) }

      describe '#build_clients' do
        it 'properly parses master' do
          client.current_master.to_s.should == 'localhost:6379'
        end

        it 'properly parses slaves' do
          client.current_slaves.first.to_s.should == 'localhost:1111'
        end
      end

      describe '#client' do
        it 'should return itself as a delegate' do
          client.client.should == client
        end
      end

      describe '#dispatch' do
        it 'routes write operations to master' do
          called = false
          client.current_master.define_singleton_method(:del) do |*args|
            called = true
          end
          client.del('foo')
          called.should ==  true
        end
      end

      describe '#inspect' do
        let(:opts) {
                      if client_type == client_type
                        {:zkservers => 'localhost:1234'}
                      else
                        {:etcd => {:host => 'localhost', :port => '1234'}}
                      end
                    }

        it 'should always include db' do
          client = client_type.new(opts)
          client.inspect.should match('<RedisFailover::Client \\[.*\\] \(db: 0,')
          db = '5'
          opts.merge!(:db => db)
          client = client_type.new(opts)
          client.inspect.should match("<RedisFailover::Client \\[.*\\] \\(db: #{db},")
        end

        it 'should include trace id' do
          tid = 'tracer'
          client = client_type.new(opts.merge(:trace_id => tid))
          client.inspect.should match("<RedisFailover::Client \\[#{tid}\\] ")
        end
      end

      describe '#call' do
        it 'should dispatch :call messages to correct method' do
          client.should_receive(:dispatch).with(:foo, *['key'])
          client.call([:foo, 'key'])
        end
      end

        context 'with :master_only false' do
          it 'routes read operations to a slave' do
            called = false
            client.current_slaves.first.change_role_to('slave')
            client.current_slaves.first.define_singleton_method(:get) do |*args|
              called = true
            end
            client.get('foo')
            called.should ==  true
          end
        end

        context 'with :master_only true' do
          it 'routes read operations to master' do
            opts = if client_type == client_type
              {:zkservers => 'localhost:9281', :master_only => true}
            else
              {:etcd => {:host => 'localhost', :port => '4001', :allow_redirect => false}}
            end

            client = client_type.new(opts)
            called = false
            client.current_master.define_singleton_method(:get) do |*args|
              called = true
            end
            client.get('foo')
            called.should ==  true
          end
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
          client.reconnected.should ==  true
        end


      describe 'redis connectivity failure handling' do
        before(:each)  do
          class << client
            attr_reader :tries

            def client_for(method)
              @tries ||= 0
              @tries += 1
              super
            end
          end
        end

        it 'retries without client rebuild when redis throws inherited error' do
          loops = 0
          client.current_master.stub(:send) {
            loops += 1
            loops < 2 ? (raise ::Redis::InheritedError) : nil
          }

          client.should_not_receive(:build_clients).with(no_args)
          client.persist('foo')
          client.tries.should be == 2
        end

        it 'retries with client rebuild when redis throws connectivity error' do
          loops = 0
          client.current_master.stub(:send) {
            loops += 1
            loops < 2 ? (raise InvalidNodeError) : nil
          }

          client.should_receive(:build_clients).with(no_args)
          client.persist('foo')
          client.tries.should be == 2
        end

        it 'throws exception when too many redis connectivity errors' do
          client.current_master.stub(:send) { raise InvalidNodeError }
          client.instance_variable_set(:@max_retries, 2)
          expect { client.persist('foo') }.to raise_error(InvalidNodeError)
        end
      end

      context 'with :verify_role true' do
        it 'properly detects when a node has changed roles' do
          client.current_master.change_role_to('slave')
          expect { client.send(:master) }.to raise_error(InvalidNodeRoleError)
        end
      end

      it 'raises error for unsupported operations' do
        expect { client.select }.to raise_error(UnsupportedOperationError)
      end

      context 'with :safe_mode enabled' do
        it 'rebuilds clients when no communication from Node Manager within certain time window' do
          client.instance_variable_set(:@last_node_timestamp, Time.at(0))
          client.should_receive(:build_clients).with(no_args)
          client.del('foo')
        end
      end

      context 'with :safe_mode disabled' do
        it 'does not rebuild clients when no communication from Node Manager within certain time window' do
          opts = if client_type == client_type
            {:zkservers => 'localhost:9281', :safe_mode => false}
          else
            {:etcd => {:host => 'localhost', :port => '4001', :verify_mode => false}}
          end

          client = client_type.new(opts)
          client.instance_variable_set(:@last_node_timestamp, Time.at(0))
          client.should_not_receive(:build_clients)
          client.del('foo')
        end
      end
    end
  end

  describe 'ExecuteFailoverClient' do
    include_examples "FailoverClient", ZkClientStub
    include_examples "FailoverClient", EtcdClientStub
  end
end

