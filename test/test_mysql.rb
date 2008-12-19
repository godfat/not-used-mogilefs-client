require 'test/setup'
require 'mogilefs/mysql'

class MogileFS::Mysql
  public :refresh_device
  public :refresh_domain
end

class TestMogileFS__Mysql < Test::Unit::TestCase

  def setup
    @my = FakeMysql.new
    @mg = MogileFS::Mysql.new(:mysql => @my)
    super
  end

  def test_refresh_device
    expect = {
     1=>
      {:hostip=>"10.0.0.1",
       :http_get_port=>7600,
       :http_port=>7500,
       :altip=>"192.168.0.1"},
     2=>
      {:hostip=>"10.0.0.2",
       :http_get_port=>7600,
       :http_port=>7500,
       :altip=>"192.168.0.2"},
     3=>
      {:hostip=>"10.0.0.3",
       :http_get_port=>7500,
       :http_port=>7500,
       :altip=>"10.0.0.3"},
     4=>
      {:hostip=>"10.0.0.4",
       :http_get_port=>7500,
       :http_port=>7500,
       :altip=>"10.0.0.4"}
    }
    assert_equal expect, @mg.refresh_device
  end

  def test_refresh_domain
    expect = { 'test' => 1, 'foo' => 2 }
    assert_equal expect, @mg.refresh_domain
  end

  def test_get_paths
    @my.expect << [ [ 12 ] ] # fid
    @my.expect << [ [ 1 ], [ 3 ] ] # devids
    expect = [ "http://10.0.0.1:7600/dev1/0/000/000/0000000012.fid",
               "http://10.0.0.3:7500/dev3/0/000/000/0000000012.fid" ]
    assert_equal expect, @mg.get_paths(:domain => 'test', :key => 'fookey')
  end

  def test_get_paths_alt
    @my.expect <<  [ [ 12 ] ] # fid
    @my.expect << [ [ 1 ], [ 3 ] ] # devids
    expect = [ "http://192.168.0.1:7600/dev1/0/000/000/0000000012.fid",
               "http://10.0.0.3:7500/dev3/0/000/000/0000000012.fid"]
    params = { :domain => 'test', :key => 'fookey', :zone => 'alt' }
    assert_equal expect, @mg.get_paths(params)
  end

  def test_list_keys
    expect_full = [ [ 'foo', 123, 2 ], [ 'bar', 456, 1 ] ]
    expect_keys = [ [ 'foo', 'bar' ], 'bar' ]
    @my.expect << expect_full
    full = []
    keys = @mg._list_keys('test') do |dkey,length,devcount|
      full << [ dkey, length, devcount ]
    end
    assert_equal expect_keys, keys
    assert_equal expect_full, full
  end

  def test_list_keys_empty
    @my.expect << []
    assert_nil @mg._list_keys('test')
  end

  def test_size
    @my.expect << [ [ '123' ] ]
    assert_equal 123, @mg._size('test', 'foo')

    @my.expect << [ [ '456' ] ]
    assert_equal 456, @mg._size('test', 'foo')
  end

  def test_sleep
    assert_nothing_raised { assert_equal({}, @mg.sleep(:duration => 1)) }
  end

end
