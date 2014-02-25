require 'rspec'
require 'ostruct'
require 'redis_failover'

Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each { |f| require f }

class NullObject
  def method_missing(method, *args, &block)
    yield if block_given?
    self
  end
end

module RedisFailover
  Util.logger = NullObject.new
end

def ZK.new(*args); NullObject.new; end

RSpec.configure do |config|
  config.filter_run :focus => true
end


