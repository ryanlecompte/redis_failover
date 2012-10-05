require 'spec_helper'

module RedisFailover
  describe FailoverStrategy do

    describe '.for' do
      it 'creates a new latency strategy instance' do
        s = FailoverStrategy.for('latency')
        s.should be_a RedisFailover::FailoverStrategy::Latency
      end

      it 'rejects unknown strategies' do
        expect { FailoverStrategy.for('foobar') }.to raise_error(RuntimeError)
      end
    end
  end
end
