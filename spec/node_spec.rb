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
    end

    describe '#reachable?' do
      it 'responds properly if node is reachable' do
        node.should be_reachable
      end

      it 'responds properly if node is unreachable' do
        node.redis.make_unreachable!
        node.should_not be_reachable
      end
    end

    describe '#unreachable?' do
      it 'responds properly if node is reachable' do
        node.should_not be_unreachable
      end

      it 'responds properly if node is unreachable' do
        node.redis.make_unreachable!
        node.should be_unreachable
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
  end
end
