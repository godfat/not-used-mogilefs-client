require 'test/setup'
require 'stringio'
require 'tempfile'
require 'fileutils'

class URI::HTTP

  class << self
    attr_accessor :read_data
    attr_accessor :open_data
  end

  def read
    self.class.read_data.shift
  end

  def open(&block)
    yield self.class.open_data
  end

end

class TestMogileFS__MogileFS < TestMogileFS

  def setup
    @klass = MogileFS::MogileFS
    super
  end

  def test_initialize
    assert_equal 'test', @client.domain
    assert_equal @root, @client.root

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

  def test_get_file_data_http_block
    tmpfp = Tempfile.new('test_mogilefs.open_data')
    nr = 100 # tested with 1000
    chunk_size = 1024 * 1024
    expect_size = nr * chunk_size
    header = "HTTP/1.0 200 OK\r\n" \
             "Content-Length: #{expect_size}\r\n\r\n"
    assert_equal header.size, tmpfp.syswrite(header)
    nr.times { assert_equal chunk_size, tmpfp.syswrite(' ' * chunk_size) }
    assert_equal expect_size + header.size, File.size(tmpfp.path)
    tmpfp.sysseek(0)
    socket = FakeSocket.new(tmpfp)
    TCPSocket.sockets << socket

    path1 = 'http://rur-1/dev1/0/000/000/0000000062.fid'
    path2 = 'http://rur-2/dev2/0/000/000/0000000062.fid'

    @backend.get_paths = { 'paths' => 2, 'path1' => path1, 'path2' => path2 }

    data = Tempfile.new('test_mogilefs.dest_data')
    @client.get_file_data('key') do |fp|
      buf = ''
      read_nr = nr = 0
      loop do
        begin
          fp.sysread(16384, buf)
          read_nr = buf.size
          nr += read_nr
          assert_equal read_nr, data.syswrite(buf), "partial write"
        rescue EOFError
          break
        end
      end
      assert_equal expect_size, nr, "size mismatch"
    end
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

  def test_size_http
    socket = FakeSocket.new <<-EOF
HTTP/1.0 200 OK\r
Content-Length: 5\r
    EOF

    TCPSocket.sockets << socket

    path = 'http://example.com/path'

    @backend.get_paths = { 'paths' => 1, 'path1' => path }

    assert_equal 5, @client.size('key')

    socket.write_s.rewind

    assert_equal "HEAD /path HTTP/1.1\r\n", socket.write_s.gets

    assert_equal ['example.com', 80], TCPSocket.connections.shift
    assert_empty TCPSocket.connections
  end

  def test_size_nfs
    path = File.join @root, 'path'

    File.open path, 'w' do |fp| fp.write 'data!' end

    @backend.get_paths = { 'paths' => 1, 'path1' => 'path' }

    assert_equal 5, @client.size('key')
  end

  def test_store_content_http
    socket = FakeSocket.new 'HTTP/1.0 200 OK'

    TCPSocket.sockets << socket

    @backend.create_open = {
      'devid' => '1',
      'path' => 'http://example.com/path',
    }

    @client.store_content 'new_key', 'test', 'data'

    expected = <<-EOF.chomp
PUT /path HTTP/1.0\r
Content-Length: 4\r
\r
data
    EOF

    assert_equal expected, socket.write_s.string

    assert_equal ['example.com', 80], TCPSocket.connections.shift
    assert_empty TCPSocket.connections
  end

  def test_store_content_http_empty
    socket = FakeSocket.new 'HTTP/1.0 200 OK'

    TCPSocket.sockets << socket

    @backend.create_open = {
      'devid' => '1',
      'path' => 'http://example.com/path',
    }

    @client.store_content 'new_key', 'test', ''

    expected = <<-EOF
PUT /path HTTP/1.0\r
Content-Length: 0\r
\r
    EOF

    assert_equal expected, socket.write_s.string

    assert_equal ['example.com', 80], TCPSocket.connections.shift
    assert_empty TCPSocket.connections
  end

  def test_store_content_nfs
    @backend.create_open = {
      'dev_count' => '1',
      'devid_1' => '1',
      'path_1' => '/path',
    }

    @client.store_content 'new_key', 'test', 'data'

    dest_file = File.join(@root, 'path')

    assert File.exist?(dest_file)
    assert_equal 'data', File.read(dest_file)
  end

  def test_store_content_nfs_empty
    @backend.create_open = {
      'dev_count' => '1',
      'devid_1' => '1',
      'path_1' => '/path',
    }

    @client.store_content 'new_key', 'test', ''

    dest_file = File.join(@root, 'path')

    assert File.exist?(dest_file)
    assert_equal '', File.read(dest_file)
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

    assert_nil @client.rename('from_key', 'to_key')
  end

  def test_rename_nonexisting
    @backend.rename = 'unknown_key', ''

    assert_nil @client.rename('from_key', 'to_key')
  end

  def test_rename_no_key
    @backend.rename = 'no_key', ''

    e = assert_raises RuntimeError do
      @client.rename 'new_key', 'test'
    end

    assert_equal 'unable to rename new_key to test: no_key', e.message
  end

  def test_rename_readonly
    @client.readonly = true

    e = assert_raises RuntimeError do
      @client.rename 'new_key', 'test'
    end

    assert_equal 'readonly mogilefs', e.message
  end

  def test_sleep
    @backend.sleep = {}
    assert_nothing_raised do
      assert_equal({}, @client.sleep(2))
    end
  end

end

