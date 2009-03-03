require 'test/setup'
require 'mogilefs/mysql'

class TestMogileFS__DbBackend < Test::Unit::TestCase
  def setup
    @my = FakeMysql.new
    @mgmy = MogileFS::Mysql.new(:mysql => @my)
    @mg = MogileFS::MogileFS.new(:db_backend => @mgmy, :domain => 'test')
  end

  def test_initialize
    assert_equal 'test', @mg.domain
    assert @mg.readonly?
  end

  def test_list_keys_block
    expect_full = [ [ 'foo', 123, 2 ], [ 'bar', 456, 1 ] ]
    expect_keys = [ [ 'foo', 'bar' ], 'bar' ]
    @my.expect << expect_full
    full = []
    keys = @mg.list_keys('test') do |dkey,length,devcount|
      full << [ dkey, length, devcount ]
    end
    assert_equal expect_keys, keys
    assert_equal expect_full, full
  end

  def test_list_keys
    expect_full = [ [ 'foo', 123, 2 ], [ 'bar', 456, 1 ] ]
    expect_keys = [ [ 'foo', 'bar' ], 'bar' ]
    @my.expect << expect_full
    keys = @mg.list_keys('test')
    assert_equal expect_keys, keys
  end

  def test_size
    @my.expect << [ [ '123' ] ]
    assert_equal 123, @mg.size('foo')

    @my.expect << [ [ '456' ] ]
    assert_equal 456, @mg.size('foo')
  end

  def test_store_file_readonly
    assert_raises(MogileFS::ReadOnlyError) do
      @mg.store_file 'new_key', 'test', '/dev/null'
    end
  end

  def test_store_content_readonly
    assert_raises(MogileFS::ReadOnlyError) do
      @mg.store_content 'new_key', 'test', 'data'
    end
  end

  def test_new_file_readonly
    assert_raises(MogileFS::ReadOnlyError) { @mg.new_file 'new_key', 'test' }
  end

  def test_rename_readonly
    assert_raises(MogileFS::ReadOnlyError) { @mg.rename 'a', 'b' }
  end

  def test_delete_readonly
    assert_raises(MogileFS::ReadOnlyError) { @mg.delete 'no_such_key' }
  end

  def test_sleep
    assert_nothing_raised { assert_equal({}, @mg.sleep(1)) }
  end

end

