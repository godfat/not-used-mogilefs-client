require 'test/setup'
require 'tempfile'

class TestMogileFS__Util < Test::Unit::TestCase
  include MogileFS::Util

  def test_mogilefs_write
    rd, wr = IO.pipe

    svr = Proc.new do |serv, port|
      client, client_addr = serv.accept
      client.sync = true
      nr = 0
      loop do
        begin
          nr += client.readpartial(16384).length
        rescue EOFError
          break
        end
      end
      wr.syswrite("#{nr}\n")
      client.close rescue nil
    end
    t = TempServer.new(svr)
    s = Socket.mogilefs_new('127.0.0.1', t.port)
    tmp = s.getsockopt(Socket::SOL_SOCKET, Socket::SO_SNDBUF)
    sndbuf_bytes = tmp.unpack('i')[0]
    big_string = ' ' * (sndbuf_bytes * 10)

    sent = s.send(big_string, 0)
    assert(sent < big_string.length)

    syswrite_full(s, big_string)
    s.close rescue nil
    IO.select([rd])
    assert_equal((sent + big_string.length), rd.sysread(4096).to_i)
  end

  def test_write_timeout
    svr = Proc.new do |serv, port|
      client, client_addr = serv.accept
      client.sync = true
      readed = client.readpartial(16384)
      sleep
    end
    t = TempServer.new(svr)
    s = Socket.mogilefs_new('127.0.0.1', t.port)
    tmp = s.getsockopt(Socket::SOL_SOCKET, Socket::SO_SNDBUF)
    sndbuf_bytes = tmp.unpack('i')[0]
    big_string = ' ' * (sndbuf_bytes * 10)

    assert_raises(MogileFS::Timeout) { syswrite_full(s, big_string, 0.1) }
    s.close rescue nil
  end

end
