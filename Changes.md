HEAD
-----------
- No longer force newly available slaves to master if already slaves of that master
- Honor a node's slave-serve-stale-data configuration option; do not mark a sync-with-master-in-progress slave as available if its slave-serve-stale-data is disabled)
- Change reachable/unreachable wording to available/unavailable

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
