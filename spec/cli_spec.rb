require 'spec_helper'

module RedisFailover
  describe CLI do
    describe '.parse' do
      it 'returns empty result for empty options' do
        CLI.parse({}).should == {}
      end

      it 'properly parses a server port' do
        opts = CLI.parse(['-P 2222'])
        opts.should == {:port => 2222}
      end

      it 'properly parses redis nodes' do
        opts = CLI.parse(['-n host1:1,host2:2,host3:3'])
        opts[:nodes].should == [
          {:host => 'host1', :port => '1'},
          {:host => 'host2', :port => '2'},
          {:host => 'host3', :port => '3'}
        ]
      end

      it 'properly parses a redis password' do
        opts = CLI.parse(['-n host:port', '-p redis_pass'])
        opts[:nodes].should == [{
          :host => 'host',
          :port => 'port',
          :password => 'redis_pass'
        }]
      end
    end
  end
end
