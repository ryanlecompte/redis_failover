require 'spec_helper'

module RedisFailover
  describe NodeStrategy do

    describe '.for' do
      it 'creates a new majority strategy instance' do
        s = NodeStrategy.for('majority')
        s.should be_a RedisFailover::NodeStrategy::Majority
      end

      it 'creates a new consensus strategy instance' do
        s = NodeStrategy.for('consensus')
        s.should be_a RedisFailover::NodeStrategy::Consensus
      end

      it 'rejects unknown strategies' do
        expect { NodeStrategy.for('foobar') }.to raise_error(RuntimeError)
      end
    end
  end
end
