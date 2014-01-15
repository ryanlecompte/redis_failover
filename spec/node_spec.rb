require 'spec_helper'

module RedisFailover
  describe Node do
    let(:node) { Node.new(:host => 'localhost', :port => '123') }

    before(:each) do
      node.extend(RedisStubSupport)
    end

    describe '#initialize' do
      it 'creates a new instance' do
        node.host.should == 'localhost'
        node.port.should == 123
      end

      it 'reports error if host missing' do
        expect { Node.new }.to raise_error(InvalidNodeError)
      end
    end

    describe '#ping' do
      it 'responds properly if node is available' do
        expect { node.ping }.to_not raise_error
      end

      it 'responds properly if node is unavailable' do
        node.redis.make_unavailable!
        expect { node.ping }.to raise_error(NodeUnavailableError)
      end
    end

    describe '#master?' do
      it 'responds properly if node is master' do
        node.should be_master
      end

      it 'responds properly if node is not master' do
        node.make_slave!(Node.new(:host => 'masterhost'))
        node.should_not be_master
      end
    end


    describe '#slave?' do
      it 'responds properly if node is slave' do
        node.should_not be_slave
      end

      it 'responds properly if node is not slave' do
        node.make_master!
        node.should be_master
      end
    end

    describe '#wait' do
      it 'should wait until node dies' do
        thread = Thread.new { node.wait }
        thread.should be_alive
        node.redis.make_unavailable!
        expect { thread.value }.to raise_error
      end
    end

    describe '#perform_operation' do
      it 'raises error for any operation that hangs for too long' do
        expect do
          node.send(:perform_operation) { 1_000_000.times { sleep 0.1 } }
        end.to raise_error(NodeUnavailableError)
      end
    end
  end
end
