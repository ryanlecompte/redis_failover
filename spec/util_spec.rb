require 'spec_helper'

module RedisFailover
  describe Util do
    describe '.symbolize_keys' do
      it 'converts hash keys to symbols' do
        Util.symbolize_keys('a' => 1, 'b' => 2).should == {:a => 1, :b => 2}
      end
    end

    describe '.different?' do
      it 'handles different arrays' do
        Util.different?([1,2,3], [1,5,3]).should == true
      end

      it 'handles non-different arrays' do
        Util.different?([1,2,3], [3,2,1]).should == false
      end
    end
  end
end
