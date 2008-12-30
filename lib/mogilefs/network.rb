require 'mogilefs'
require 'socket'

module MogileFS::Network
  # given an array of URIs, verify that at least one of them is accessible
  # with the expected HTTP code within the timeout period (in seconds).
  def verify_uris(uris = [], expect = '200', timeout = 2.00)
    uri_socks = {}
    ok_uris = []
    sockets = []

    # first, we asynchronously connect to all of them
    uris.each do |uri|
      sock = Socket.mogilefs_new_nonblock(uri.host, uri.port) rescue next
      uri_socks[sock] = uri
    end

    # wait for at least one of them to finish connecting and send
    # HTTP requests to the connected ones
    begin
      t0 = Time.now
      r = IO.select(nil, uri_socks.keys, nil, timeout > 0 ? timeout : 0)
      timeout -= (Time.now - t0)
      break unless r && r[1]
      r[1].each do |sock|
        begin
          sock.syswrite "HEAD #{uri_socks[sock].request_uri} HTTP/1.0\r\n\r\n"
          sockets << sock
        rescue
          sock.close rescue nil
        end
      end
    end until sockets[0] || timeout < 0

    # Await a response from the sockets we had written to, we only need one
    # valid response, but we'll take more if they return simultaneously
    if sockets[0]
      begin
        t0 = Time.now
        r = IO.select(sockets, nil, nil, timeout > 0 ? timeout : 0)
        timeout -= (Time.now - t0)
        break unless r && r[0]
        r[0].each do |sock|
          buf = sock.recv_nonblock(128, Socket::MSG_PEEK) rescue next
          if buf && /\AHTTP\/[\d\.]+ #{expect} / =~ buf
            ok_uris << uri_socks.delete(sock)
            sock.close rescue nil
          end
        end
      end
    end until ok_uris[0] || timeout < 0

    ok_uris
    ensure
      uri_socks.keys.each { |sock| sock.close rescue nil }
  end

end # module MogileFS::Network
