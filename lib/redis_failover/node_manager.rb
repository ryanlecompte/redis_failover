module RedisFailover
  Dir[File.dirname(__FILE__) + '/node_managers/*.rb'].sort.each {|file| require file}

  # NodeManager manages a list of redis nodes. Upon startup, the NodeManager
  # will discover the current redis master and slaves. Each redis node is
  # monitored by a NodeWatcher instance. The NodeWatchers periodically
  # report the current state of the redis node it's watching to the
  # NodeManager. The NodeManager processes the state reports and reacts
  # appropriately by handling stale/dead nodes, and promoting a new redis master
  # if it sees fit to do so.
  class NodeManager
    def self.new(args)
      raise ArgumentError, "args: #{args.inspect} must be a hash." unless args.is_a?(::Hash)

      args[:config_store] == "etcd" ? EtcdNodeManager.new(args) : ZookeeperNodeManager.new(args)
    end
  end
end
