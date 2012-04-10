require 'sinatra'

module RedisFailover
  # Serves as an endpoint for discovering the current redis master and slaves.
  class Server < Sinatra::Base
    disable :logging

    get '/redis_servers' do
      content_type :json
      MultiJson.encode(Runner.node_manager.nodes)
    end
  end
end
