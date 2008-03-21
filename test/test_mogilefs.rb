require 'test/setup'

class URI::HTTP

  class << self
    attr_accessor :read_data
  end

  def read
    self.class.read_data.shift
  end

end

class TestMogileFS__MogileFS < TestMogileFS

  def setup
    @klass = MogileFS::MogileFS
    super
  end

  def test_initialize
    assert_equal 'test', @client.domain
    assert_equal '/mogilefs/test', @client.root

    assert_raises ArgumentError do
      MogileFS::MogileFS.new :hosts => ['kaa:6001'], :root => '/mogilefs/test'
    end
  end

  def test_get_file_data_http
    URI::HTTP.read_data = %w[data!]

    path1 = 'http://rur-1/dev1/0/000/000/0000000062.fid'
    path2 = 'http://rur-2/dev2/0/000/000/0000000062.fid'

    @backend.get_paths = { 'paths' => 2, 'path1' => path1, 'path2' => path2 }

    assert_equal 'data!', @client.get_file_data('key')
  end

  def test_get_paths
    path1 = 'rur-1/dev1/0/000/000/0000000062.fid'
    path2 = 'rur-2/dev2/0/000/000/0000000062.fid'

    @backend.get_paths = { 'paths' => 2, 'path1' => path1, 'path2' => path2 }

    expected = ["#{@root}/#{path1}", "#{@root}/#{path2}"]

    assert_equal expected, @client.get_paths('key').sort
  end

  def test_get_paths_unknown_key
    @backend.get_paths = ['unknown_key', '']

    assert_equal nil, @client.get_paths('key')
  end

  def test_delete_existing
    @backend.delete = { }
    assert_nothing_raised do
      @client.delete 'no_such_key'
    end
  end

  def test_delete_nonexisting
    @backend.delete = 'unknown_key', ''
    assert_nothing_raised do
      assert_equal nil, @client.delete('no_such_key')
    end
  end

  def test_delete_readonly
    @client.readonly = true
    assert_raises RuntimeError do
      @client.delete 'no_such_key'
    end
  end

  def test_each_key
    @backend.list_keys = { 'key_count' => 2, 'next_after' => 'new_key_2',
                           'key_1' => 'new_key_1', 'key_2' => 'new_key_2' }
    @backend.list_keys = { 'key_count' => 2, 'next_after' => 'new_key_4',
                           'key_1' => 'new_key_3', 'key_2' => 'new_key_4' }
    @backend.list_keys = { 'key_count' => 0, 'next_after' => 'new_key_4' }
    keys = []
    @client.each_key 'new' do |key|
      keys << key
    end

    assert_equal %w[new_key_1 new_key_2 new_key_3 new_key_4], keys
  end

  def test_list_keys
    @backend.list_keys = { 'key_count' => 2, 'next_after' => 'new_key_2',
                           'key_1' => 'new_key_1', 'key_2' => 'new_key_2' }

    keys, next_after = @client.list_keys 'new'
    assert_equal ['new_key_1', 'new_key_2'], keys.sort
    assert_equal 'new_key_2', next_after
  end

  def test_new_file_http
    @client.readonly = true
    assert_raises RuntimeError do
      @client.new_file 'new_key', 'test'
    end
  end

  def test_new_file_readonly
    @client.readonly = true
    assert_raises RuntimeError do
      @client.new_file 'new_key', 'test'
    end
  end

  def test_store_content_readonly
    @client.readonly = true
    assert_raises RuntimeError do
      @client.store_content 'new_key', 'test', nil
    end
  end

  def test_store_file_readonly
    @client.readonly = true
    assert_raises RuntimeError do
      @client.store_file 'new_key', 'test', nil
    end
  end

  def test_rename_existing
    @backend.rename = {}
    assert_nothing_raised do
      assert_equal(nil, @client.rename('from_key', 'to_key'))
    end
  end

  def test_rename_nonexisting
    @backend.rename = 'unknown_key', ''
    assert_nothing_raised do
      assert_equal(nil, @client.rename('from_key', 'to_key'))
    end
  end

  def test_rename_readonly
    @client.readonly = true
    assert_raises RuntimeError do
      @client.rename 'new_key', 'test'
    end
  end

  def test_sleep
    @backend.sleep = {}
    assert_nothing_raised do
      assert_equal({}, @client.sleep(2))
    end
  end

end

