require_relative 'client_impl'
module RedisFailover
  # Redis failover-aware client. RedisFailover::Client is a wrapper over a set
  # of underlying redis clients, which means all normal redis operations can be
  # performed on an instance of this class. The class only requires a set of
  # ZooKeeper server addresses to function properly. The client will automatically
  # retry failed operations, and handle failover to a new master. The client
  # registers and listens for watcher events from the Node Manager. When these
  # events are received, the client fetches the latest set of redis nodes from
  # ZooKeeper and rebuilds its internal Redis clients appropriately.
  # RedisFailover::Client also directs write operations to the master, and all
  # read operations to the slaves.
  #
  # @example Usage
  #   zk_servers = 'localhost:2181,localhost:2182,localhost:2183'
  #   client = RedisFailover::Client.new(:zkservers => zk_servers)
  #   client.set('foo', 1) # will be directed to master
  #   client.get('foo') # will be directed to a slave
  #
  class EtcdClient < ClientImpl
  end
end
