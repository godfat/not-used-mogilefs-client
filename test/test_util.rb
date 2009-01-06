require 'test/setup'
require 'tempfile'

class TestMogileFS__Util < Test::Unit::TestCase
  include MogileFS::Util

  def test_mogilefs_write
    done = Queue.new

    svr = Proc.new do |serv, port|
      client, client_addr = serv.accept
      client.sync = true
      readed = 0
      loop do
        begin
          readed += client.readpartial(16384).length
        rescue EOFError
          break
        end
      end
      done << readed
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
    readed = done.pop

    assert_equal((sent + big_string.length), readed)
  end

end
