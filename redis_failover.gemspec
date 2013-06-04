# -*- encoding: utf-8 -*-
require File.expand_path('../lib/redis_failover/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Ryan LeCompte"]
  gem.email         = ["lecompte@gmail.com"]
  gem.description   = %(redis_failover is a ZooKeeper-based automatic master/slave failover solution for Ruby)
  gem.summary       = %(redis_failover is a ZooKeeper-based automatic master/slave failover solution for Ruby)
  gem.homepage      = "http://github.com/ryanlecompte/redis_failover"

  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.files         = `git ls-files`.split("\n")
  gem.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.name          = "redis_failover"
  gem.require_paths = ["lib"]
  gem.version       = RedisFailover::VERSION

  gem.add_dependency('redis', ['>= 2.2', '< 4'])
  gem.add_dependency('redis-namespace')
  gem.add_dependency('multi_json', '~> 1')
  gem.add_dependency('zk', ['>= 1.7.4', '< 2.0'])

  gem.add_development_dependency('rake')
  gem.add_development_dependency('rspec')
  gem.add_development_dependency('yard')
end
