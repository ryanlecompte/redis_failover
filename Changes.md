HEAD
-----------
- redis_failover now supports distributed monitoring among the node managers! Previously, the node managers were only used
as a means of redundancy in case a particular node manager crashed. Starting with version 1.0 of redis_failover, the node
managers will all periodically report their health report/snapshots. The primary node manager will utilize a configurable
"node strategy" to determine if a particular node is available or unavailable.
- redis_failover now supports a configurable "failover strategy" that's consulted when performing a failover. Currently,
a single strategy is provided that takes into account the average latency of the last health check to the redis server.

0.9.7.2
-----------
- Add support for Redis#client's location method. Fixes a compatibility issue with redis_failover and Sidekiq.

0.9.7.1
-----------
- Stop repeated attempts to acquire exclusive lock in Node Manager (#36)

0.9.7
-----------
- Stubbed Client#client to return itself, fixes a fork reconnect bug with Resque (dbalatero)

0.9.6
-----------
- Handle the node discovery error condition where the znode points to a master that is now a slave.

0.9.5
-----------
- Introduce a safer master node discovery process for the Node Manager (#34)
- Improved shutdown process for Node Manager

0.9.4
-----------
- Preserve original master by reading from existing znode state.
- Prevent Timeout::Error from bringing down the process (#32) (@eric)

0.9.3
-----------
- Add lock assert for Node Manager.

0.9.2
-----------
- Improved exception handling in NodeWatcher.

0.9.1
-----------
- Improve nested exception handling.
- Fix manual failover support when znode does not exist first.
- Various fixes to work better with 1.8.7.

0.9.0
-----------
- Make Node Manager's lock path vary with its main znode. (Bira)
- Node Manager's znode for holding current list of redis nodes is no longer ephemeral. This is unnecessary since the current master should only be changed by redis_failover.
- Introduce :master_only option for RedisFailover::Client (disabled by default). This option configures the client to direct all read/write operations to the master.
- Introduce :safe_mode option (enabled by default). This option configures the client to purge its redis clients when a ZK session expires or when the client hasn't recently heard from the node manager.
- Introduce RedisFailover::Client#on_node_change callback notification for when the currently known list of master/slave redis nodes changes.
- Added #current_master and #current_slaves to RedisFailover::Client. This is useful for programmatically doing things based on the current master/slaves.
- redis_node_manager should start if no redis servers are available (#29)
- Better handling of ZK session expirations in Node Manager.

0.8.9
-----------
- Handle errors raised by redis 3.x client (tsilen) 

0.8.8
-----------
- Use a stack for handling nested blocks in RedisFailover::Client (inspired by connection_pool gem)
- Fix an issue with #multi and Redis 3.x.

0.8.7
-----------
- Support TTL operation (#24)

0.8.6
-----------
- No longer buffer output (kyohsuke)
- Update redis/zk gem versions to latest (rudionrails)

0.8.5
-----------
- Lock-down gemspec to version 1.1.x of redis-namespace to play nicely with redis 2.2.x.
- Fix RedisFailover::Client#manual_failover regression (oleriesenberg)

0.8.4
-----------
- Lock-down gemspec to redis 2.2.x in light of upcoming redis 3.x release. Once sufficient testing
has been done with the 3.x release, I will relax the constraint in the gemspec.
- Add environment-scoped configuration file support (oleriesenberg)

0.8.3
-----------
- Added a way to gracefully shutdown/reconnect a RedisFailover::Client. (#13)
- Upgraded to latest ZK version that supports forking.
- Handle case where the same RedisFailover::Client is referenced by a #multi block (#14)

0.8.2
-----------
- Fix method signature for RedisFailover::Client#respond_to_missing? (#12)

0.8.1
-----------
- Added YARD documentation.
- Improve ZooKeeper client connection management.
- Upgrade to latest ZK gem stable release.

0.8.0
-----------
- Added manual failover support (can be initiated via RedisFailover::Client#manual_failover)
- Misc. cleanup

0.7.0
-----------
- When new master promotion occurs, make existing slaves point to new candidate before promoting new master.
- Add support for specifying command-line options in a config.yml file for Node Manager.
- Upgrade to 0.9 version of ZK client and cleanup ZK connection error handling.

0.6.0
-----------
- Add support for running multiple Node Manager processes for added redundancy (#4)
- Add support for specifying a redis database in RedisFailover::Client (#5)
- Improved Node Manager command-line option parsing

0.5.4
-----------
- No longer use problematic ZK#reopen.

0.5.3
-----------
- Handle more ZK exceptions as candidates for reconnecting the client on error.
- Add safety check to actively purge redis clients if a RedisFailover::Client hasn't heard from the Node Manager in a certain time window.

0.5.2
-----------
- Always try to create path before setting current state in Node Manager.
- More explicit rescuing of exceptions.

0.5.1
-----------
- More logging around exceptions
- Handle re-watching on client session expirations / disconnections
- Use an ephemeral node for the list of redis servers

0.5.0
-----------
- redis_failover is now built on top of ZooKeeper! This means redis_failover enjoys all of the reliability, redundancy, and data consistency offered by ZooKeeper. The old fragile HTTP-based approach has been removed and will no longer be supported in favor of ZooKeeper. This does mean that in order to use redis_failover, you must have ZooKeeper installed and running. Please see the README for steps on how to do this if you don't already have ZooKeeper running in your production environment.

0.4.0
-----------
- No longer force newly available slaves to master if already slaves of that master
- Honor a node's slave-serve-stale-data configuration option; do not mark a sync-with-master-in-progress slave as available if its slave-serve-stale-data is disabled
- Change reachable/unreachable wording to available/unavailable
- Added node reconciliation, i.e. if a node comes back up, make sure that the node manager and the node agree on current role
- More efficient use of redis client connections
- Raise proper error for unsupported operations (i.e., those that don't make sense for a failover client)
- Properly handle any hanging node operations in the failover server

0.3.0
-----------
- Integrated travis-ci
- Added background monitor to client for proactively detecting changes to current set of redis nodes

0.2.0
-----------
- Added retry support for contacting failover server from client
- Client now verifies proper master/slave role before attempting operation
- General edge case cleanup for NodeManager

0.1.1
-----------

- Fix option parser require

0.1.0
-----------

- First release
