require 'test/unit'
require 'test/setup'

$TESTING = true

require 'mogilefs/backend'

class MogileFS::Backend

  attr_accessor :hosts
  attr_reader :timeout, :dead
  attr_writer :lasterr, :lasterrstr, :socket

end

class TestBackend < Test::Unit::TestCase

  def setup
    @backend = MogileFS::Backend.new :hosts => ['localhost:1']
  end

  def test_initialize
    assert_raises ArgumentError do MogileFS::Backend.new end
    assert_raises ArgumentError do MogileFS::Backend.new :hosts => [] end
    assert_raises ArgumentError do MogileFS::Backend.new :hosts => [''] end

    assert_equal ['localhost:1'], @backend.hosts
    assert_equal 3, @backend.timeout
    assert_equal nil, @backend.lasterr
    assert_equal nil, @backend.lasterrstr
    assert_equal({}, @backend.dead)

    @backend = MogileFS::Backend.new :hosts => ['localhost:6001'], :timeout => 1
    assert_equal 1, @backend.timeout
  end

  def test_do_request
    received = Tempfile.new('received')
    tmp = TempServer.new(Proc.new do |serv, port|
      client, client_addr = serv.accept
      client.sync = true
      received.syswrite(client.recv(4096))
      client.send "OK 1 you=win\r\n", 0
    end)

    @backend.hosts = [ "127.0.0.1:#{tmp.port}" ]

    assert_equal({'you' => 'win'},
                 @backend.do_request('go!', { 'fight' => 'team fight!' }))
    received.sysseek(0)
    assert_equal "go! fight=team+fight%21\r\n", received.sysread(4096)
    ensure
      TempServer.destroy_all!
  end

  def test_do_request_send_error
    socket_request = ''
    socket = Object.new
    def socket.closed?() false end
    def socket.send(request, flags) raise SystemCallError, 'dummy' end

    @backend.instance_variable_set '@socket', socket

    assert_raises MogileFS::UnreachableBackendError do
      @backend.do_request 'go!', { 'fight' => 'team fight!' }
    end

    assert_equal nil, @backend.instance_variable_get('@socket')
  end

  def test_automatic_exception
    assert ! MogileFS::Backend.const_defined?('PebkacError')
    assert @backend.error('pebkac')
    assert_equal MogileFS::Error, @backend.error('PebkacError').superclass
    assert MogileFS::Backend.const_defined?('PebkacError')

    assert ! MogileFS::Backend.const_defined?('PebKacError')
    assert @backend.error('peb_kac')
    assert_equal MogileFS::Error, @backend.error('PebKacError').superclass
    assert MogileFS::Backend.const_defined?('PebKacError')
  end

  def test_size_verify_error_defined
    # "ErrorError" just looks dumb, but we used to get it
    # since mogilefs would send us "size_verify_error" and we'd
    # blindly append "Error" to the exception
    assert ! MogileFS::Backend.const_defined?('SizeVerifyErrorError')
    assert MogileFS::Backend.const_defined?('SizeVerifyError')
  end

  def test_do_request_truncated
    socket_request = ''
    socket = Object.new
    def socket.closed?() false end
    def socket.send(request, flags) return request.length - 1 end

    @backend.instance_variable_set '@socket', socket

    assert_raises MogileFS::RequestTruncatedError do
      @backend.do_request 'go!', { 'fight' => 'team fight!' }
    end
  end

  def test_make_request
    assert_equal "go! fight=team+fight%21\r\n",
                 @backend.make_request('go!', { 'fight' => 'team fight!' })
  end

  def test_parse_response
    assert_equal({'foo' => 'bar', 'baz' => 'hoge'},
                 @backend.parse_response('OK 1 foo=bar&baz=hoge'))

    err = nil
    begin
      @backend.parse_response('ERR you totally suck')
    rescue MogileFS::Error => err
      assert_equal 'MogileFS::Backend::YouError', err.class.to_s
      assert_equal 'totally suck', err.message
    end
    assert_equal 'MogileFS::Backend::YouError', err.class.to_s

    assert_equal 'you', @backend.lasterr
    assert_equal 'totally suck', @backend.lasterrstr

    assert_raises MogileFS::InvalidResponseError do
      @backend.parse_response 'garbage'
    end
  end

  def test_readable_eh_readable
    accept = Tempfile.new('accept')
    tmp = TempServer.new(Proc.new do |serv, port|
      client, client_addr = serv.accept
      client.sync = true
      accept.syswrite('.')
      client.send('.', 0)
      sleep
    end)

    @backend = MogileFS::Backend.new :hosts => [ "127.0.0.1:#{tmp.port}" ]
    assert_equal true, @backend.readable?
    assert_equal 1, accept.stat.size
    ensure
      TempServer.destroy_all!
  end

  def test_readable_eh_not_readable
    tmp = TempServer.new(Proc.new { |serv,port| serv.accept; sleep })
    @backend = MogileFS::Backend.new(:hosts => [ "127.0.0.1:#{tmp.port}" ],
                                     :timeout => 0.5)
    begin
      @backend.readable?
    rescue MogileFS::UnreadableSocketError => e
      assert_equal "127.0.0.1:#{tmp.port} never became readable", e.message
    rescue Exception => err
      flunk "MogileFS::UnreadableSocketError not raised #{err} #{err.backtrace}"
    else
      flunk "MogileFS::UnreadableSocketError not raised"
    ensure
      TempServer.destroy_all!
    end
  end

  def test_socket
    assert_equal({}, @backend.dead)
    assert_raises MogileFS::UnreachableBackendError do @backend.socket end
    assert_equal(['localhost:1'], @backend.dead.keys)
  end

  def test_socket_robust
    bad_accept = Tempfile.new('bad_accept')
    accept = Tempfile.new('accept')
    bad = Proc.new do |serv,port|
      client, client_addr = serv.accept
      bad_accept.syswrite('!')
    end
    good = Proc.new do |serv,port|
      client, client_addr = serv.accept
      accept.syswrite('.')
      client.syswrite('.')
      client.close
      sleep
    end
    nr = 10

    nr.times do
      begin
        t1 = TempServer.new(bad, ENV['TEST_DEAD_PORT'])
        t2 = TempServer.new(good)
        hosts = ["127.0.0.1:#{t1.port}", "127.0.0.1:#{t2.port}"]
        @backend = MogileFS::Backend.new(:hosts => hosts.dup)
        assert_equal({}, @backend.dead)
        old_chld_handler = trap('CHLD', 'DEFAULT')
        t1.destroy!
        Process.waitpid(t1.pid)
        trap('CHLD', old_chld_handler)
        sock = @backend.socket
        assert_equal Socket, sock.class
        port = Socket.unpack_sockaddr_in(sock.getpeername).first
        # p [ 'ports', "port=#{port}", "t1=#{t1.port}", "t2=#{t2.port}" ]
        assert_equal t2.port, port
        IO.select([sock])
        assert_equal '.', sock.sysread(1)
      ensure
        TempServer.destroy_all!
      end
    end # nr.times
    assert_equal 0, bad_accept.stat.size
    assert_equal nr, accept.stat.size
  end

  def test_shutdown
    accept_nr = 0
    tmp = TempServer.new(Proc.new do |serv,port|
      client, client_addr = serv.accept
      accept_nr += 1
      r = IO.select([client], [client])
      client.syswrite(accept_nr.to_s)
      sleep
    end)
    @backend = MogileFS::Backend.new :hosts => [ "127.0.0.1:#{tmp.port}" ]
    assert @backend.socket
    assert ! @backend.socket.closed?
    IO.select([@backend.socket])
    resp = @backend.socket.sysread(4096)
    @backend.shutdown
    assert_equal nil, @backend.instance_variable_get(:@socket)
    assert_equal 1, resp.to_i
    ensure
      TempServer.destroy_all!
  end

  def test_url_decode
    assert_equal({"\272z" => "\360opy", "f\000" => "\272r"},
                 @backend.url_decode("%baz=%f0opy&f%00=%bar"))
    assert_equal({}, @backend.url_decode(''))
  end

  def test_url_encode
    params = [["f\000", "\272r"], ["\272z", "\360opy"]]
    assert_equal "f%00=%bar&%baz=%f0opy", @backend.url_encode(params)
  end

  def test_url_escape # \n for unit_diff
    actual = (0..255).map { |c| @backend.url_escape c.chr }.join "\n"

    expected = []
    expected.push(*(0..0x1f).map { |c| "%%%0.2x" % c })
    expected << '+'
    expected.push(*(0x21..0x2b).map { |c| "%%%0.2x" % c })
    expected.push(*%w[, - . /])
    expected.push(*('0'..'9'))
    expected.push(*%w[: %3b %3c %3d %3e %3f %40])
    expected.push(*('A'..'Z'))
    expected.push(*%w[%5b \\ %5d %5e _ %60])
    expected.push(*('a'..'z'))
    expected.push(*(0x7b..0xff).map { |c| "%%%0.2x" % c })

    expected = expected.join "\n"

    assert_equal expected, actual
  end

  def test_url_unescape
    input = []
    input.push(*(0..0x1f).map { |c| "%%%0.2x" % c })
    input << '+'
    input.push(*(0x21..0x2b).map { |c| "%%%0.2x" % c })
    input.push(*%w[, - . /])
    input.push(*('0'..'9'))
    input.push(*%w[: %3b %3c %3d %3e %3f %40])
    input.push(*('A'..'Z'))
    input.push(*%w[%5b \\ %5d %5e _ %60])
    input.push(*('a'..'z'))
    input.push(*(0x7b..0xff).map { |c| "%%%0.2x" % c })

    actual = input.map { |c| @backend.url_unescape c }.join "\n"

    expected = (0..255).map { |c| c.chr }.join "\n"
    expected.sub! '+', ' '

    assert_equal expected, actual
  end

end

