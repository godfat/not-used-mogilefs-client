require 'test/unit'
require 'pp'
require 'socket'
$TESTING = true
require 'mogilefs'
require 'mogilefs/util'

class TestUtils < Test::Unit::TestCase
  include MogileFS::Util

  def test_verify_uris
    good_serv, good_port = server_start
    good_acceptor = Thread.new do
      good_client, good_client_addr = good_serv.accept
      good_client.readpartial(4096)
      good_client.syswrite("HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n")
    end
    bad_serv, bad_port = server_start
    bad_acceptor = Thread.new do
      bad_client, bad_client_addr = bad_serv.accept
      bad_client.close rescue nil
    end

    good_uri = URI.parse("http://127.0.0.1:#{good_port}/")
    bad_uri = URI.parse("http://127.0.0.1:#{bad_port}/")
    ok = verify_uris([ good_uri, bad_uri ])
    assert_equal [ good_uri ], ok
    ensure
      Thread.kill(good_acceptor) rescue nil
      Thread.kill(bad_acceptor) rescue nil
      good_serv.close rescue nil
      bad_serv.close rescue nil
  end

  private

    def server_start
      port = nil
      sock = nil
      retries = 0
      begin
        port = 5000 + $$ % 1000 + rand(60000)
        sock = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
        sock.bind(Socket.pack_sockaddr_in(port, '127.0.0.1'))
        sock.listen(5)
      rescue Errno::EADDRINUSE
        sock.close rescue nil
        retry if (retries += 1) < 10
      end
      [ sock, port ]
    end

end
