# Automatic Redis Failover Client / Node Manager

[![Build Status](https://secure.travis-ci.org/ryanlecompte/redis_failover.png?branch=master)](http://travis-ci.org/ryanlecompte/redis_failover)

redis_failover attempts to provides a full automatic master/slave failover solution for Ruby. Redis does not provide
an automatic failover capability when configured for master/slave replication. When the master node dies,
a new master must be manually brought online and assigned as the slave's new master. This manual
switch-over is not desirable in high traffic sites where Redis is a critical part of the overall
architecture. The existing standard Redis client for Ruby also only supports configuration for a single
Redis server. When using master/slave replication, it is desirable to have all writes go to the
master, and all reads go to one of the N configured slaves.

This gem attempts to address these failover scenarios. A redis failover Node Manager daemon runs as a background
process and monitors all of your configured master/slave nodes. When the daemon starts up, it
automatically discovers the current master/slaves. Background watchers are setup for each of
the redis nodes. As soon as a node is detected as being offline, it will be moved to an "unavailable" state.
If the node that went offline was the master, then one of the slaves will be promoted as the new master.
All existing slaves will be automatically reconfigured to point to the new master for replication.
All nodes marked as unavailable will be periodically checked to see if they have been brought back online.
If so, the newly available nodes will be configured as slaves and brought back into the list of available
nodes. Note that detection of a node going down should be nearly instantaneous, since the mechanism
used to keep tabs on a node is via a blocking Redis BLPOP call (no polling). This call fails nearly
immediately when the node actually goes offline. To avoid false positives (i.e., intermittent flaky
network interruption), the Node Manager will only mark a node as unavailable if it fails to communicate with
it 3 times (this is configurable via --max-failures, see configuration options below).

This gem provides a RedisFailover::Client wrapper that is master/slave aware. The client is configured
with a list of ZooKeeper servers. The client will automatically contact the ZooKeeper cluster to find out
the current state of the world (i.e., who is the current master and who are the current slaves). The client
also sets up a ZooKeeper watcher for the set of redis nodes controlled by the Node Manager daemon. When the daemon
promotes a new master or detects a node as going down, ZooKeeper will notify the client near-instantaneously so
that it can rebuild its set of Redis connections. The client also acts as a load balancer in that it will automatically
dispatch Redis read operations to one of N slaves, and Redis write operations to the master.
If it fails to communicate with any node, it will go back and fetch the current list of available servers, and then
optionally retry the operation.

## Installation

redis_failover has an external dependency on ZooKeeper. You must have a running ZooKeeper cluster already available in order to use redis_failover. ZooKeeper provides redis_failover with its high availability and data consistency between Redis::Failover clients and the Node Manager daemon. Please see the requirements section below for more information on installing and setting up ZooKeeper if you don't have it running already.

Add this line to your application's Gemfile:

    gem 'redis_failover'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install redis_failover

## Node Manager Daemon Usage

The Node Manager is a simple process that should be run as a background daemon. The daemon supports the
following options:

    Usage: redis_node_manager [OPTIONS]
        -p, --password password          Redis password (optional)
        -n, --nodes redis nodes          Comma-separated redis host:port pairs (required)
        -z zookeeper servers,            Comma-separated zookeeper host:port pairs (required)
            --zkservers
            --znode-path path            Znode path override for storing redis server list (optional)
            --max-failures count         Max failures before manager marks node unavailable (default 3)
        -h, --help                       Display all options

To start the daemon for a simple master/slave configuration, use the following:

    redis_node_manager -n localhost:6379,localhost:6380 -z localhost:2181,localhost:2182,localhost:2183

The Node Manager will automatically discover the master/slaves upon startup. Note that it is
a good idea to monitor the redis Node Manager daemon process with a tool like Monit to ensure that it is restarted
in the case of a failure.

## Client Usage

The redis failover client must be used in conjunction with a running Node Manager daemon. The
client supports various configuration options, however the only mandatory option is the list of
ZooKeeper servers:

    client = RedisFailover::Client.new(:zkservers => 'localhost:2181,localhost:2182,localhost:2183')

The client actually employs the common redis and redis-namespace gems underneath, so this should be
a drop-in replacement for your existing pure redis client usage.

The full set of options that can be passed to RedisFailover::Client are:

     :zkservers     - comma-separated zookeeper host:port pairs (required)
     :znode_path    - the Znode path override for redis server list (optional)
     :password      - password for redis nodes (optional)
     :namespace     - namespace for redis nodes (optional)
     :logger        - logger override (optional)
     :retry_failure - indicate if failures should be retried (default true)
     :max_retries   - max retries for a failure (default 3)

## Requirements

- redis_failover is actively tested against MRI 1.9.2/1.9.3. Other rubies may work, although I don't actively test against them. 1.8 is not supported.
- redis_failover requires a ZooKeeper service cluster to ensure reliability and data consistency. ZooKeeper is very simple and easy to get up and running. Please refer to this [Zookeper Guide](http://zookeeper.apache.org/doc/trunk/zookeeperStarted.html) to get up and running quickly if you don't already have ZooKeeper as a part of your environment.

## Considerations

Note that by default the Node Manager will mark slaves that are currently syncing with their master as "available" based on the configuration value set for "slave-serve-stale-data" in redis.conf. By default this value is set to "yes" in the configuration, which means that slaves still syncing with their master will be available for servicing read requests. If you don't want this behavior, just set "slave-serve-stale-data" to "no" in your redis.conf file.

## TODO

- Rework specs to work against a set of real Redis/ZooKeeper nodes as opposed to stubs.

## Resources

- To learn more about Redis master/slave replication, see the [Redis documentation](http://redis.io/topics/replication).
- To learn more about ZooKeeper, see the official [ZooKeeper](http://zookeeper.apache.org/) site.

## License

Please see LICENSE for licensing details.

## Author

Ryan LeCompte

[@ryanlecompte](https://twitter.com/ryanlecompte)

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
