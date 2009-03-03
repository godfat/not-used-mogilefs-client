require 'test/setup'
require 'mogilefs'
require 'mogilefs/network'

class TestNetwork < Test::Unit::TestCase
  include MogileFS::Network

  def test_verify_uris
    good = TempServer.new(Proc.new do |serv,port|
      client,client_addr = serv.accept
      client.readpartial(4096)
      client.syswrite("HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n")
    end)
    bad = TempServer.new(Proc.new do |serv,port|
      client, client_addr = serv.accept
      client.close rescue nil
    end)

    good_uri = URI.parse("http://127.0.0.1:#{good.port}/")
    bad_uri = URI.parse("http://127.0.0.1:#{bad.port}/")
    ok = verify_uris([ good_uri, bad_uri ])
    assert_equal [ good_uri ], ok
    ensure
      TempServer.destroy_all!
  end

  def test_verify_non_existent
    good = TempServer.new(Proc.new do |serv,port|
      client,client_addr = serv.accept
      client.readpartial(4096)
      client.syswrite("HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n")
    end)
    bad = TempServer.new(Proc.new { |serv,port| serv.close })

    good_uri = URI.parse("http://127.0.0.1:#{good.port}/")
    bad_uri = URI.parse("http://127.0.0.1:#{bad.port}/")
    ok = verify_uris([ good_uri, bad_uri ])
    assert_equal [ good_uri ], ok
    ensure
      TempServer.destroy_all!
  end

  def test_verify_all_bad
    good = TempServer.new(Proc.new { |serv,port| serv.close })
    bad = TempServer.new(Proc.new { |serv,port| serv.close })

    good_uri = URI.parse("http://127.0.0.1:#{good.port}/")
    bad_uri = URI.parse("http://127.0.0.1:#{bad.port}/")
    ok = verify_uris([ good_uri, bad_uri ], '200', 1.0)
    assert ok.empty?, "nothing returned"
    ensure
      TempServer.destroy_all!
  end

end
