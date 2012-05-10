HEAD
-----------
- Added a way to gracefully shutdown a RedisFailover::Client. (#13)
- Upgraded to latest ZK version that supports forking.

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
