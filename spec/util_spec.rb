require 'spec_helper'

module RedisFailover
  describe Util do
    describe '.symbolize_keys' do
      it 'converts hash keys to symbols' do
        Util.symbolize_keys('a' => 1, 'b' => 2).should == {:a => 1, :b => 2}
      end
    end
  end
end
