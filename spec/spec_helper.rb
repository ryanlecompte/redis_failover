require 'rspec'
require 'redis_failover'

Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each { |f| require f }

class NullObject
  def method_missing(method, *args, &block)
    self
  end
end

module RedisFailover
  Util.logger = NullObject.new
  def ZkClient.new(*args); NullObject.new; end
end

RSpec.configure do |config|
end