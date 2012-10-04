require 'spec_helper'

module RedisFailover
  describe Strategy do

    describe '.for' do
      it 'creates a new majority strategy instance' do
        s = Strategy.for('majority')
        s.should be_a RedisFailover::Strategy::Majority
      end

      it 'creates a new consensus strategy instance' do
        s = Strategy.for('consensus')
        s.should be_a RedisFailover::Strategy::Consensus
      end

      it 'rejects unknown strategies' do
        expect { Strategy.for('foobar') }.to raise_error(RuntimeError)
      end
    end
  end
end
