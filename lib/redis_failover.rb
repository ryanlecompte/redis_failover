require 'zk'

#NOTE: We've found that using the 'recommended' zk fork-hook would trigger
#kernel mutex deadlocks in forking env (unicorn & resque) [ruby 1.9]
#https://github.com/zk-ruby/zk/wiki/Forking & https://github.com/zk-ruby/zk/blob/master/RELEASES.markdown#v150
#ZK.install_fork_hook

require 'set'
require 'yaml'
require 'redis'
require 'thread'
require 'logger'
require 'timeout'
require 'optparse'
require 'benchmark'
require 'multi_json'
require 'securerandom'

require 'redis_failover/cli'
require 'redis_failover/util'
require 'redis_failover/node'
require 'redis_failover/errors'
require 'redis_failover/client'
require 'redis_failover/runner'
require 'redis_failover/version'
require 'redis_failover/node_manager'
require 'redis_failover/node_watcher'
require 'redis_failover/node_strategy'
require 'redis_failover/node_snapshot'
require 'redis_failover/manual_failover'
require 'redis_failover/failover_strategy'
