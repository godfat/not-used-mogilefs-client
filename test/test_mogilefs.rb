require 'test/setup'
require 'stringio'
require 'tempfile'
require 'fileutils'

class TestMogileFS__MogileFS < TestMogileFS
  include MogileFS::Util

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
    accept_nr = 0
    svr = Proc.new do |serv, port|
      client, client_addr = serv.accept
      client.sync = true
      readed = client.recv(4096, 0)
      assert(readed =~ \
            %r{\AGET /dev[12]/0/000/000/0000000062\.fid HTTP/1.[01]\r\n\r\n\Z})
      client.send("HTTP/1.0 200 OK\r\nContent-Length: 5\r\n\r\ndata!", 0)
      accept_nr += 1
      client.close
    end
    t1 = TempServer.new(svr)
    t2 = TempServer.new(svr)
    path1 = "http://127.0.0.1:#{t1.port}/dev1/0/000/000/0000000062.fid"
    path2 = "http://127.0.0.1:#{t2.port}/dev2/0/000/000/0000000062.fid"

    @backend.get_paths = { 'paths' => 2, 'path1' => path1, 'path2' => path2 }

    assert_equal 'data!', @client.get_file_data('key')
    assert_equal 1, accept_nr
    ensure
      TempServer.destroy_all!
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

    accept_nr = 0
    svr = Proc.new do |serv, port|
      client, client_addr = serv.accept
      client.sync = true
      accept_nr += 1
      readed = client.recv(4096, 0)
      assert(readed =~ \
            %r{\AGET /dev[12]/0/000/000/0000000062\.fid HTTP/1.[01]\r\n\r\n\Z})
      syswrloop(tmpfp, client)
      client.close
    end
    t1 = TempServer.new(svr)
    t2 = TempServer.new(svr)
    path1 = "http://127.0.0.1:#{t1.port}/dev1/0/000/000/0000000062.fid"
    path2 = "http://127.0.0.1:#{t2.port}/dev2/0/000/000/0000000062.fid"

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
      assert_equal 1, accept_nr
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

    assert_raises MogileFS::Backend::UnknownKeyError do
      assert_equal nil, @client.get_paths('key')
    end
  end

  def test_delete_existing
    @backend.delete = { }
    assert_nothing_raised do
      @client.delete 'no_such_key'
    end
  end

  def test_delete_nonexisting
    @backend.delete = 'unknown_key', ''
    assert_raises MogileFS::Backend::UnknownKeyError do
      @client.delete('no_such_key')
    end
  end

  def test_delete_readonly
    @client.readonly = true
    assert_raises MogileFS::ReadOnlyError do
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

  def test_list_keys_block
    @backend.list_keys = { 'key_count' => 2, 'next_after' => 'new_key_2',
                           'key_1' => 'new_key_1', 'key_2' => 'new_key_2' }
    http_resp = "HTTP/1.0 200 OK\r\nContent-Length: %u\r\n"
    srv = Proc.new do |serv, port, size|
      client, client_addr = serv.accept
      client.sync = true
      readed = client.readpartial(4096)
      assert %r{\AHEAD } =~ readed
      client.send(http_resp % size, 0)
      client.close
    end
    t1 = TempServer.new(Proc.new { |serv, port| srv.call(serv, port, 5) })
    t2 = TempServer.new(Proc.new { |serv, port| srv.call(serv, port, 5) })
    t3 = TempServer.new(Proc.new { |serv, port| srv.call(serv, port, 10) })
    @backend.get_paths = { 'paths' => 2,
                           'path1' => "http://127.0.0.1:#{t1.port}/",
                           'path2' => "http://127.0.0.1:#{t2.port}/" }
    @backend.get_paths = { 'paths' => 1,
                           'path1' => "http://127.0.0.1:#{t3.port}/" }

    res = []
    keys, next_after = @client.list_keys('new') do |key,length,devcount|
      res << [ key, length, devcount ]
    end

    expect_res = [ [ 'new_key_1', 5, 2 ], [ 'new_key_2', 10, 1 ] ]
    assert_equal expect_res, res
    assert_equal ['new_key_1', 'new_key_2'], keys.sort
    assert_equal 'new_key_2', next_after
    ensure
      TempServer.destroy_all!
  end

  def test_new_file_http
    @client.readonly = true
    assert_raises MogileFS::ReadOnlyError do
      @client.new_file 'new_key', 'test'
    end
  end

  def test_new_file_readonly
    @client.readonly = true
    assert_raises MogileFS::ReadOnlyError do
      @client.new_file 'new_key', 'test'
    end
  end

  def test_size_http
    accept_nr = 0
    t = TempServer.new(Proc.new do |serv,port|
      client, client_addr = serv.accept
      client.sync = true
      readed = client.recv(4096, 0) rescue nil
      assert_equal "HEAD /path HTTP/1.0\r\n\r\n", readed
      client.send("HTTP/1.0 200 OK\r\nContent-Length: 5\r\n\r\n", 0)
      accept_nr += 1
      client.close
    end)

    path = "http://127.0.0.1:#{t.port}/path"
    @backend.get_paths = { 'paths' => 1, 'path1' => path }

    assert_equal 5, @client.size('key')
    assert_equal 1, accept_nr
  end

  def test_size_nfs
    path = File.join @root, 'path'

    File.open path, 'w' do |fp| fp.write 'data!' end

    @backend.get_paths = { 'paths' => 1, 'path1' => 'path' }

    assert_equal 5, @client.size('key')
  end

  def test_store_content_http
    received = ''
    expected = "PUT /path HTTP/1.0\r\nContent-Length: 4\r\n\r\ndata"

    t = TempServer.new(Proc.new do |serv, accept|
      client, client_addr = serv.accept
      client.sync = true
      received = client.recv(4096, 0)
      client.send("HTTP/1.0 200 OK\r\n\r\n", 0)
      client.close
    end)

    @backend.create_open = {
      'devid' => '1',
      'path' => "http://127.0.0.1:#{t.port}/path",
    }

    @client.store_content 'new_key', 'test', 'data'

    assert_equal expected, received
    ensure
      TempServer.destroy_all!
  end

  def test_store_content_http_fail
    t = TempServer.new(Proc.new do |serv, accept|
      client, client_addr = serv.accept
      client.sync = true
      client.recv(4096, 0)
      client.send("HTTP/1.0 500 Internal Server Error\r\n\r\n", 0)
      client.close
    end)

    @backend.create_open = {
      'devid' => '1',
      'path' => "http://127.0.0.1:#{t.port}/path",
    }

    assert_raises MogileFS::HTTPFile::BadResponseError do
      @client.store_content 'new_key', 'test', 'data'
    end
  end

  def test_store_content_http_empty
    received = ''
    expected = "PUT /path HTTP/1.0\r\nContent-Length: 0\r\n\r\n"
    t = TempServer.new(Proc.new do |serv, accept|
      client, client_addr = serv.accept
      client.sync = true
      received = client.recv(4096, 0)
      client.send("HTTP/1.0 200 OK\r\n\r\n", 0)
      client.close
    end)

    @backend.create_open = {
      'devid' => '1',
      'path' => "http://127.0.0.1:#{t.port}/path",
    }

    @client.store_content 'new_key', 'test', ''
    assert_equal expected, received
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

  def test_new_file_http_large
    expect = Tempfile.new('test_mogilefs.expect')
    to_put = Tempfile.new('test_mogilefs.to_put')
    received = Tempfile.new('test_mogilefs.received')

    nr = 10 # tested with 1000
    chunk_size = 1024 * 1024
    expect_size = nr * chunk_size

    header = "PUT /path HTTP/1.0\r\n" \
             "Content-Length: #{expect_size}\r\n\r\n"
    assert_equal header.size, expect.syswrite(header)
    nr.times do
      assert_equal chunk_size, expect.syswrite(' ' * chunk_size)
      assert_equal chunk_size, to_put.syswrite(' ' * chunk_size)
    end
    assert_equal expect_size + header.size, expect.stat.size
    assert_equal expect_size, to_put.stat.size

    readed = 0
    t = TempServer.new(Proc.new do |serv, accept|
      client, client_addr = serv.accept
      client.sync = true
      loop do
        buf = client.readpartial(8192) or break
        break if buf.length == 0
        assert_equal buf.length, received.syswrite(buf)
        readed += buf.length
        break if readed >= expect.stat.size
      end
      client.send("HTTP/1.0 200 OK\r\n\r\n", 0)
      client.close
    end)

    @backend.create_open = {
      'devid' => '1',
      'path' => "http://127.0.0.1:#{t.port}/path",
    }

    @client.store_file('new_key', 'test', to_put.path)
    assert_equal expect.stat.size, readed

    ENV['PATH'].split(/:/).each do |path|
      cmp_bin = "#{path}/cmp"
      File.executable?(cmp_bin) or next
      # puts "running #{cmp_bin} #{expect.path} #{received.path}"
      assert( system(cmp_bin, expect.path, received.path) )
      break
    end

    ensure
      TempServer.destroy_all!
  end

  def test_store_content_readonly
    @client.readonly = true

    assert_raises MogileFS::ReadOnlyError do
      @client.store_content 'new_key', 'test', nil
    end
  end

  def test_store_file_readonly
    @client.readonly = true
    assert_raises MogileFS::ReadOnlyError do
      @client.store_file 'new_key', 'test', nil
    end
  end

  def test_rename_existing
    @backend.rename = {}

    assert_nil @client.rename('from_key', 'to_key')
  end

  def test_rename_nonexisting
    @backend.rename = 'unknown_key', ''

    assert_raises MogileFS::Backend::UnknownKeyError do
      @client.rename('from_key', 'to_key')
    end
  end

  def test_rename_no_key
    @backend.rename = 'no_key', 'no_key'

    e = assert_raises MogileFS::Backend::NoKeyError do
      @client.rename 'new_key', 'test'
    end

    assert_equal 'no_key', e.message
  end

  def test_rename_readonly
    @client.readonly = true

    e = assert_raises MogileFS::ReadOnlyError do
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

