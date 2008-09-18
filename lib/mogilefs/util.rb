module MogileFS::Util

  CHUNK_SIZE = 65536

  # for copying large files while avoiding GC thrashing as much as possible
  def sysrwloop(io_rd, io_wr, filter = nil)
    copied = 0
    # avoid making sysread repeatedly allocate a new String
    # This is not well-documented, but both read/sysread can take
    # an optional second argument to use as the buffer to avoid
    # GC overhead of creating new strings in a loop
    buf = ' ' * CHUNK_SIZE # preallocate to avoid GC thrashing
    io_wr.sync = true
    loop do
      begin
        b = io_rd.sysread(CHUNK_SIZE, buf)
        b = filter.call(b) if filter
        copied += syswrite_full(io_wr, b)
      rescue EOFError
        break
      end
    end

    # filter must take nil as a possible argument to indicate EOF
    if filter
      b = filter.call(nil)
      copied += syswrite_full(io_wr, b) if b && b.length
    end
    copied
  end # sysrwloop

  def verify_uris(uris = [], expect = '200', timeout = 2.00)
    uri_socks = {}
    ok_uris = []

    uris.each do |uri|
      sock = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
      sock.fcntl(Fcntl::F_SETFL, sock.fcntl(Fcntl::F_GETFL) | Fcntl::O_NONBLOCK)
      begin
        sock.connect(Socket.pack_sockaddr_in(uri.port, uri.host))
        uri_socks[sock] = uri
      rescue
        sock.close rescue nil
      end
    end

    sockets = []
    begin
      t0 = Time.now
      r = select(nil, uri_socks.keys, nil, timeout > 0 ? timeout : 0)
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

    if sockets[0]
      begin
        t0 = Time.now
        r = select(sockets, nil, nil, timeout > 0 ? timeout : 0)
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

  private

    def syswrite_full(io_wr, buf)
      written = 0
      loop do
        w = io_wr.syswrite(buf)
        written += w
        break if w == buf.size
        buf = buf[w..-1]
      end

      written
    end

end
