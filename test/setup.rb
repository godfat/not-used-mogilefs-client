require 'test/unit'

$TESTING = true

require 'mogilefs'

class FakeBackend

  attr_reader :lasterr, :lasterrstr

  def initialize
    @responses = Hash.new { |h,k| h[k] = [] }
    @lasterr = nil
    @lasterrstr = nil
  end

  def method_missing(meth, *args)
    meth = meth.to_s
    if meth =~ /(.*)=$/ then
      @responses[$1] << args.first
    else
      response = @responses[meth].shift
      case response
      when Array then
        @lasterr = response.first
        @lasterrstr = response.last
        return nil
      end
      return response
    end
  end

end

class MogileFS::Client
  attr_writer :readonly
end

class TestMogileFS < Test::Unit::TestCase

  def setup
    return if self.class == TestMogileFS
    @root = '/mogilefs/test'
    @client = @klass.new :hosts => ['kaa:6001'], :domain => 'test',
                                  :root => @root
    @backend = FakeBackend.new
    @client.instance_variable_set '@backend', @backend
  end

  def test_nothing
  end

end

