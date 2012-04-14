# Automatic Redis Failover Client/Server

[![Build Status](https://secure.travis-ci.org/ryanlecompte/redis_failover.png?branch=master)](http://travis-ci.org/ryanlecompte/redis_failover)

Redis Failover attempts to provides a full automatic master/slave failover solution for Ruby. Redis does not provide
an automatic failover capability when configured for master/slave replication. When the master node dies,
a new master must be manually brought online and assigned as the slave's new master. This manual
switch-over is not desirable in high traffic sites where Redis is a critical part of the overall
architecture. The existing standard Redis client for Ruby also only supports configuration for a single
Redis server. When using master/slave replication, it is desirable to have all writes go to the
master, and all reads go to one of the N configured slaves.

This gem attempts to address both the server and client problems. A redis failover server runs as a background
daemon and monitors all of your configured master/slave nodes. When the server starts up, it
automatically discovers who is the master and who are the slaves. Watchers are setup for each of
the redis nodes. As soon as a node is detected as being offline, it will be moved to an "unavailable" state.
If the node that went offline was the master, then one of the slaves will be promoted as the new master.
All existing slaves will be automatically reconfigured to point to the new master for replication.
All nodes marked as unavailable will be periodically checked to see if they have been brought back online.
If so, the newly available nodes will be configured as slaves and brought back into the list of live
servers. Note that detection of a node going down should be nearly instantaneous, since the mechanism
used to keep tabs on a node is via a blocking Redis BLPOP call (no polling). This call fails nearly
immediately when the node actually goes offline. To avoid false positives (i.e., intermittent flaky
network interruption), the server will only mark a node as unavailable if it fails to communicate with
it 3 times (this is configurable via --max-failures, see configuration options below).

This gem provides a RedisFailover::Client wrapper that is master/slave aware. The client is configured
with a single host/port pair that points to redis failover server. The client will automatically
connect to the server to find out the current state of the world (i.e., who's the current master and
who are the current slaves). The client also acts as a load balancer in that it will automatically
dispatch Redis read operations to one of N slaves, and Redis write operations to the master.
If it fails to communicate with any node, it will go back and ask the server for the current list of
available servers, and then optionally retry the operation.

## Installation

Add this line to your application's Gemfile:

    gem 'redis_failover'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install redis_failover

## Server Usage

The redis failover server is a simple process that should be run as a background daemon. The server supports the
following options:

    Usage: redis_failover_server [OPTIONS]
        -P, --port port                  Server port
        -p, --password password          Redis password
        -n, --nodes nodes                Comma-separated redis host:port pairs
            --max-failures count         Max failures before server marks node unavailable (default 3)
        -h, --help                       Display all options

To start the server for a simple master/slave configuration, use the following:

    redis_failover_server -P 3000 -n localhost:6379,localhost:6380

The server will automatically figure out who is the master and who is the slave upon startup. Note that it is
a good idea to monitor the redis failover server process with a tool like Monit to ensure that it is restarted
in the case of a failure.

## Client Usage

The redis failover client must be used in conjunction with a running redis failover server. The
client supports various configuration options, however the two mandatory options are the host
and port of the redis failover server:

    client = RedisFailover::Client.new(:host => 'localhost', :port => 3000)

The client actually employs the common redis and redis-namespace gems underneath, so this should be
a drop-in replacement for your existing pure redis client usage.

The full set of options that can be passed to RedisFailover::Client are:

     :host          - redis failover server host (required)
     :port          - redis failover server port (required)
     :password      - optional password for redis nodes
     :namespace     - optional namespace for redis nodes
     :logger        - optional logger override
     :retry_failure - indicate if failures should be retried (default true)
     :max_retries   - max retries for a failure (default 3)

## Limitations

The redis_failover gem currently has limitations. It currently does not gracefully handle network partitions. In cases where
the network splits, it is possible that more than one master could exist until the failover server sees all of the nodes again.
If the failover client gets split from the failover server, it's also possible that it could be talking to a stale master. This would get corrected once the client could successfully reach the failover server again to fetch the latest set of master/slave nodes. This is a limitation that I hope to address in a future release. The gem can not guarantee data consistencies until this is addressed.

## Resources

To learn more about Redis master/slave replication, see the [Redis documentation](http://redis.io/topics/replication).

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
