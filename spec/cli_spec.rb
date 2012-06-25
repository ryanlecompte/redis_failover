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

      it 'properly parses a chroot' do
        opts = CLI.parse(['-n host:port', '-z localhost:1111', '--with-chroot', '/with/chroot/from/command/line'])
        opts[:nodes].should == [{
          :host => 'host',
          :port => 'port',
        }]

        opts[:chroot].should == '/with/chroot/from/command/line'
      end

      it 'properly parses the config file' do
        opts = CLI.parse(['-C', "#{File.dirname(__FILE__)}/support/config/single_environment.yml"])
        opts[:zkservers].should == 'zk01:2181,zk02:2181,zk03:2181'

        opts = CLI.parse(['-C', "#{File.dirname(__FILE__)}/support/config/multiple_environments.yml", '-E', 'development'])
        opts[:zkservers].should == 'localhost:2181'

        opts = CLI.parse(['-C', "#{File.dirname(__FILE__)}/support/config/multiple_environments.yml", '-E', 'staging'])
        opts[:zkservers].should == 'zk01:2181,zk02:2181,zk03:2181'
      end

      it 'properly parses the config file that include chroot' do
        opts = CLI.parse(['-C', "#{File.dirname(__FILE__)}/support/config/single_environment_with_chroot.yml"])
        opts[:zkservers].should == 'zk01:2181,zk02:2181,zk03:2181'
        opts[:chroot].should == '/with/chroot'

        opts = CLI.parse(['-C', "#{File.dirname(__FILE__)}/support/config/multiple_environments_with_chroot.yml", '-E', 'development'])
        opts[:zkservers].should == 'localhost:2181'
        opts[:chroot].should == '/with/chroot_development'

        opts = CLI.parse(['-C', "#{File.dirname(__FILE__)}/support/config/multiple_environments_with_chroot.yml", '-E', 'staging'])
        opts[:zkservers].should == 'zk01:2181,zk02:2181,zk03:2181'
        opts[:chroot].should == '/with/chroot_staging'
      end
    end
  end
end
