require 'spec_helper'

module RedisFailover
  describe CLI do
    describe '.parse' do
      it 'properly parses redis nodes' do
        opts = CLI.parse(['-n host1:1,host2:2,host3:3', '-z localhost:1111'])
        opts[:nodes].should == [
          {:host => 'host1', :port => '1'},
          {:host => 'host2', :port => '2'},
          {:host => 'host3', :port => '3'}
        ]
      end

      it 'properly parses ZooKeeper servers' do
        opts = CLI.parse(['-n host1:1,host2:2,host3:3', '-z localhost:1111'])
        opts[:zkservers].should == 'localhost:1111'
      end

      it 'properly parses a redis password' do
        opts = CLI.parse(['-n host:port', '-z localhost:1111', '-p redis_pass'])
        opts[:nodes].should == [{
          :host => 'host',
          :port => 'port',
          :password => 'redis_pass'
        }]
      end

      it 'properly parses max node failures' do
        opts = CLI.parse([
          '-n host:port',
          '-z localhost:1111',
          '-p redis_pass',
          '--max-failures',
          '1'])
        opts[:max_failures].should == 1
      end
    end
  end
end
