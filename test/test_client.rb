require 'test/unit'
require 'mogilefs'

class MogileFS::Backend
  attr_accessor :timeout, :lasterr, :lasterrstr, :hosts
end

class MogileFS::Client
  attr_accessor :hosts
end

class TestClient < Test::Unit::TestCase

  def setup
    @client = MogileFS::Client.new :hosts => ['kaa:6001']
  end

  def test_initialize
    client = MogileFS::Client.new :hosts => ['kaa:6001']
    assert_not_nil client
    assert_instance_of MogileFS::Backend, client.backend
    assert_equal ['kaa:6001'], client.hosts

    client = MogileFS::Client.new :hosts => ['kaa:6001'], :timeout => 5
    assert_equal 5, client.backend.timeout
  end

  def test_err
    @client.backend.lasterr = 'you'
    assert_equal 'you', @client.err
  end

  def test_errstr
    @client.backend.lasterrstr = 'totally suck'
    assert_equal 'totally suck', @client.errstr
  end

  def test_reload
    orig_backend = @client.backend

    @client.hosts = ['ziz:6001']
    @client.reload

    assert_not_same @client.backend, orig_backend
    assert_equal ['ziz:6001'], @client.backend.hosts
  end

  def test_readonly_eh_readonly
    client = MogileFS::Client.new :hosts => ['kaa:6001'], :readonly => true
    assert_equal true, client.readonly?
  end

  def test_readonly_eh_readwrite
    assert_equal false, @client.readonly?
  end

end

