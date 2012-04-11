# -*- encoding: utf-8 -*-
require File.expand_path('../lib/redis_failover/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Ryan LeCompte"]
  gem.email         = ["lecompte@gmail.com"]
  gem.description   = %(Redis Failover provides a full automatic master/slave failover solution for Ruby)
  gem.summary       = %(Redis Failover provides a full automatic master/slave failover solution for Ruby)
  gem.homepage      = "http://github.com/ryanlecompte/redis_failover"

  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.files         = `git ls-files`.split("\n")
  gem.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.name          = "redis_failover"
  gem.require_paths = ["lib"]
  gem.version       = RedisFailover::VERSION

  gem.add_dependency('redis')
  gem.add_dependency('redis-namespace')
  gem.add_dependency('multi_json')
  gem.add_dependency('sinatra')

  gem.add_development_dependency('rake')
  gem.add_development_dependency('rspec')
end
