# Automatic Redis Failover

[![Build Status](https://secure.travis-ci.org/ryanlecompte/redis_failover.png?branch=master)](http://travis-ci.org/ryanlecompte/redis_failover)

redis_failover attempts to provides a full automatic master/slave failover solution for Ruby. Redis does not currently provide
an automatic failover capability when configured for master/slave replication. When the master node dies,
a new master must be manually brought online and assigned as the slave's new master. This manual
switch-over is not desirable in high traffic sites where Redis is a critical part of the overall
architecture. The existing standard Redis client for Ruby also only supports configuration for a single
Redis server. When using master/slave replication, it is desirable to have all writes go to the
master, and all reads go to one of the N configured slaves.

This gem (built using [ZK][]) attempts to address these failover scenarios. One or more Node Manager daemons run as background
processes and monitor all of your configured master/slave nodes. When the daemon starts up, it
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
it 3 times (this is configurable via --max-failures, see configuration options below). Note that you can (and should)
deploy multiple Node Manager daemons since they each report periodic health reports/snapshots of the redis servers. A
"node strategy" is used to determine if a node is actually unavailable. By default a majority strategy is used, but
you can also configure "consensus" or "single" as well.

This gem provides a RedisFailover::Client wrapper that is master/slave aware. The client is configured
with a list of ZooKeeper servers. The client will automatically contact the ZooKeeper cluster to find out
the current state of the world (i.e., who is the current master and who are the current slaves). The client
also sets up a ZooKeeper watcher for the set of redis nodes controlled by the Node Manager daemon. When the daemon
promotes a new master or detects a node as going down, ZooKeeper will notify the client near-instantaneously so
that it can rebuild its set of Redis connections. The client also acts as a load balancer in that it will automatically
dispatch Redis read operations to one of N slaves, and Redis write operations to the master.
If it fails to communicate with any node, it will go back and fetch the current list of available servers, and then
optionally retry the operation.

[ZK]: https://github.com/slyphon/zk

## Architecture Diagram

![redis_failover architecture diagram](https://github.com/ryanlecompte/redis_failover/raw/master/misc/redis_failover.png)

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


    Specific options:
        -n, --nodes NODES                  Comma-separated redis host:port pairs
        -z, --zkservers SERVERS            Comma-separated ZooKeeper host:port pairs
        -p, --password PASSWORD            Redis password
            --znode-path PATH              Znode path override for storing redis server list
            --max-failures COUNT           Max failures before manager marks node unavailable
        -C, --config PATH                  Path to YAML config file
            --with-chroot ROOT             Path to ZooKeepers chroot
        -E, --environment ENV              Config environment to use
            --node-strategy STRATEGY       Strategy used when determining availability of nodes (default: majority)
            --failover-strategy STRATEGY   Strategy used when failing over to a new node (default: latency)
            --required-node-managers COUNT Required Node Managers that must be reachable to determine node state (default: 1)
        -h, --help                         Display all options

To start the daemon for a simple master/slave configuration, use the following:

    redis_node_manager -n localhost:6379,localhost:6380 -z localhost:2181,localhost:2182,localhost:2183

The configuration parameters can also be specified in a config.yml file. An example configuration
would look like the following:

    ---
    :max_failures: 2
    :node_strategy: majority
    :failover_strategy: latency
    :required_node_managers: 2
    :nodes:
      - localhost:6379
      - localhost:1111
      - localhost:2222
      - localhost:3333
    :zkservers:
      - localhost:2181
      - localhost:2182
      - localhost:2183
    :password: foobar

You would then simpy start the Node Manager via the following:

    redis_node_manager -C config.yml

You can also scope the configuration to a particular environment (e.g., staging/development). See the examples
directory for configuration file samples.

The Node Manager will automatically discover the master/slaves upon startup. Note that it is
a good idea to run more than one instance of the Node Manager daemon in your environment. At
any moment, a single Node Manager process will be designated to manage the redis servers. If
this Node Manager process dies or becomes partitioned from the network, another Node Manager
will be promoted as the primary manager of redis servers. You can run as many Node Manager
processes as you'd like. Every Node Manager periodically records health "snapshots" which the
primary/master Node Manager consults when determining if it should officially mark a redis 
server as unavailable. By default, a majority strategy is used. Also, when a failover
happens, the primary Node Manager will consult the node snapshots to determine the best
node to use as the new master.

## Client Usage

The redis failover client must be used in conjunction with a running Node Manager daemon. The
client supports various configuration options, however the only mandatory option is the list of
ZooKeeper servers OR an existing ZK client instance:

    # Explicitly specify the ZK servers
    client = RedisFailover::Client.new(:zkservers => 'localhost:2181,localhost:2182,localhost:2183')

    # Explicitly specify an existing ZK client instance (useful if using a connection pool, etc)
    zk = ZK.new('localhost:2181,localhost:2182,localhost:2183')
    client = RedisFailover::Client.new(:zk => zk)

The client actually employs the common redis and redis-namespace gems underneath, so this should be
a drop-in replacement for your existing pure redis client usage.

The full set of options that can be passed to RedisFailover::Client are:

     :zk            - an existing ZK client instance
     :zkservers     - comma-separated ZooKeeper host:port pairs
     :znode_path    - the Znode path override for redis server list (optional)
     :password      - password for redis nodes (optional)
     :db            - db to use for redis nodes (optional)
     :namespace     - namespace for redis nodes (optional)
     :logger        - logger override (optional)
     :retry_failure - indicate if failures should be retried (default true)
     :max_retries   - max retries for a failure (default 3)
     :safe_mode     - indicates if safe mode is used or not (default true)
     :master_only   - indicates if only redis master is used (default false)

The RedisFailover::Client also supports a custom callback that will be invoked whenever the list of redis clients changes. Example usage:

    RedisFailover::Client.new(:zkservers => 'localhost:2181,localhost:2182,localhost:2183') do |client|
      client.on_node_change do |master, slaves|
        logger.info("Nodes changed! master: #{master}, slaves: #{slaves}")
      end
    end

## Manual Failover

Manual failover can be initiated via RedisFailover::Client#manual_failover. This schedules a manual failover with the
currently active Node Manager. Once the Node Manager receives the request, it will either failover to the specific
server passed to #manual_failover, or it will pick a random slave to become the new master. Here's an example:

    client = RedisFailover::Client.new(:zkservers => 'localhost:2181,localhost:2182,localhost:2183')
    client.manual_failover(:host => 'localhost', :port => 2222)

## Node & Failover Strategies

As of redis_failover version 1.0, the notion of "node" and "failover" strategies exists. All running Node Managers will periodically record
"snapshots" of their view of the redis nodes. The primary Node Manager will process these snapshots from all of the Node Managers by running a configurable
node strategy. By default, a majority strategy is used. This means that if a majority of Node Managers indicate that a node is unavailable, then the primary
Node Manager will officially mark it as unavailable. Other strategies exist:

- consensus (all Node Managers must agree that the node is unavailable)
- single (at least one Node Manager saying the node is unavailable will cause the node to be marked as such)

When a failover happens, the primary Node Manager will now consult a "failover strategy" to determine which candidate node should be used. Currently only a single
strategy is provided by redis_failover: latency. This strategy simply selects a node that is both marked as available by all Node Managers and has the lowest
average latency for its last health check.

Note that you should set the "required_node_managers" configuration option appropriately. This value (defaults to 1) is used to determine if enough Node
Managers have reported their view of a node's state. For example, if you have deployed 5 Node Managers, then you should set this value to 5 if you only
want to accept a node's availability when all 5 Node Managers are part of the snapshot. To give yourself flexibility, you may want to set this value to 3
instead. This would give you flexibility to take down 2 Node Managers, while still allowing the cluster to be managed appropriately.

## Documentation

redis_failover uses YARD for its API documentation. Refer to the generated [API documentation](http://rubydoc.info/github/ryanlecompte/redis_failover/master/frames) for full coverage.

## Requirements

- redis_failover is actively tested against MRI 1.8.7/1.9.2/1.9.3 and JRuby 1.6.7 (1.9 mode only). Other rubies may work, although I don't actively test against them.
- redis_failover requires a ZooKeeper service cluster to ensure reliability and data consistency. ZooKeeper is very simple and easy to get up and running. Please refer to this [Quick ZooKeeper Guide](https://github.com/ryanlecompte/redis_failover/wiki/Quick-ZooKeeper-Guide) to get up and running quickly if you don't already have ZooKeeper as a part of your environment.

## Considerations

- Note that by default the Node Manager will mark slaves that are currently syncing with their master as "available" based on the configuration value set for "slave-serve-stale-data" in redis.conf. By default this value is set to "yes" in the configuration, which means that slaves still syncing with their master will be available for servicing read requests. If you don't want this behavior, just set "slave-serve-stale-data" to "no" in your redis.conf file.

## Limitations

- Note that it's still possible for the RedisFailover::Client instances to see a stale list of servers for a very small window. In most cases this will not be the case due to how ZooKeeper handles distributed communication, but you should be aware that in the worst case the client could write to a "stale" master for a small period of time until the next watch event is received by the client via ZooKeeper.

## Resources

- Check out Steve Whittaker's [redis-failover-test](https://github.com/swhitt/redis-failover-test) project which shows how to test redis_failover in a non-trivial configuration using Vagrant/Chef.
- To learn more about Redis master/slave replication, see the [Redis documentation](http://redis.io/topics/replication).
- To learn more about ZooKeeper, see the official [ZooKeeper](http://zookeeper.apache.org/) site.
- See the [Quick ZooKeeper Guide](https://github.com/ryanlecompte/redis_failover/wiki/Quick-ZooKeeper-Guide) for a quick guide to getting ZooKeeper up and running with redis_failover.
- To learn more about how ZooKeeper handles network partitions, see [ZooKeeper Failure Scenarios](http://wiki.apache.org/hadoop/ZooKeeper/FailureScenarios)
- Slides for a [lightning talk](http://www.slideshare.net/ryanlecompte/handling-redis-failover-with-zookeeper) that I gave at BaRuCo 2012.
- Feel free to join #zk-gem on the IRC freenode network. We're usually hanging out there talking about ZooKeeper and redis_failover.


## License

Please see LICENSE for licensing details.

## Author

Ryan LeCompte

[@ryanlecompte](https://twitter.com/ryanlecompte)

## Acknowledgements

Special thanks to [Eric Lindvall](https://github.com/eric) and [Jonathan Simms](https://github.com/slyphon) for their invaluable ZooKeeper advice and guidance!

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
