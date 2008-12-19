module MogileFS::Util

  CHUNK_SIZE = 65536

  # for copying large files while avoiding GC thrashing as much as possible
  # writes the contents of io_rd into io_wr, running through filter if
  # it is a Proc object.  The filter proc must respond to a string
  # argument (and return a string) and to nil (possibly returning a
  # string or nil).  This can be used to filter I/O through an
  # Zlib::Inflate or Digest::MD5 object
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
        b = begin
          io_rd.sysread(CHUNK_SIZE, buf)
        rescue Errno::EAGAIN, Errno::EINTR
          select([io_rd], nil, nil, nil)
          retry
        end
        b = filter.call(b) if filter
        copied += syswrite_full(io_wr, b)
      rescue EOFError
        break
      end
    end

    # filter must take nil as a possible argument to indicate EOF
    if filter
      b = filter.call(nil)
      copied += syswrite_full(io_wr, b) if b && b.length > 0
    end
    copied
  end # sysrwloop

  # given an array of URIs, verify that at least one of them is accessible
  # with the expected HTTP code within the timeout period (in seconds).
  def verify_uris(uris = [], expect = '200', timeout = 2.00)
    uri_socks = {}
    ok_uris = []
    sockets = []

    # first, we asynchronously connect to all of them
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

    # wait for at least one of them to finish connecting and send
    # HTTP requests to the connected ones
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

    # Await a response from the sockets we had written to, we only need one
    # valid response, but we'll take more if they return simultaneously
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

    # writes the contents of buf to io_wr in full w/o blocking
    def syswrite_full(io_wr, buf)
      written = 0
      loop do
        w = begin
          io_wr.syswrite(buf)
        rescue Errno::EAGAIN, Errno::EINTR
          select(nil, [io_wr], nil, nil)
          retry
        end
        written += w
        break if w == buf.size
        buf = buf[w..-1]
      end

      written
    end

end
