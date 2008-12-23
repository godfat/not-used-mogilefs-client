require 'mogilefs'
require 'socket'

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
      sock = Socket.mogilefs_new_nonblock(uri.host, uri.port) rescue next
      uri_socks[sock] = uri
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

require 'timeout'
##
# Timeout error class.  Subclassing it from Timeout::Error is the only
# reason we require the 'timeout' module, otherwise that module is
# broken and worthless to us.
class MogileFS::Timeout < Timeout::Error; end

class Socket
  attr_accessor :mogilefs_addr, :mogilefs_connected

  # Socket lacks peeraddr method of the IPSocket/TCPSocket classes
  def mogilefs_peername
    Socket.unpack_sockaddr_in(getpeername).reverse.map {|x| x.to_s }.join(':')
  end

  def mogilefs_init(host = nil, port = nil)
    return true if defined?(@mogilefs_connected)

    @mogilefs_addr = Socket.sockaddr_in(port, host).freeze if port && host

    begin
      connect_nonblock(@mogilefs_addr)
      @mogilefs_connected = true
    rescue Errno::EINPROGRESS
      nil
    rescue Errno::EISCONN
      @mogilefs_connected = true
    end
  end

  class << self

    # Creates a new (TCP) Socket and initiates (but does not wait for) the
    # connection
    def mogilefs_new_nonblock(host, port)
      sock = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
      sock.sync = true
      sock.mogilefs_init(host, port)
      sock
    end

    # Like TCPSocket.new(host, port), but with an explicit timeout
    # (and we don't care for local address/port we're binding to).
    # This raises MogileFS::Timeout if timeout expires
    def mogilefs_new(host, port, timeout = 5.0)
      sock = mogilefs_new_nonblock(host, port) or return sock

      while timeout > 0
        t0 = Time.now
        r = IO.select(nil, [sock], nil, timeout)
        return sock if r && r[1] && sock.mogilefs_init
        timeout -= (Time.now - t0)
      end

      sock.close rescue nil
      raise MogileFS::Timeout, 'socket write timeout'
    end

    # Makes a request on a new TCP Socket and returns with a readble socket
    # within the given timeout.
    # This raises MogileFS::Timeout if timeout expires
    def mogilefs_new_request(host, port, request, timeout = 5.0)
      t0 = Time.now
      sock = mogilefs_new(host, port, timeout)
      sock.syswrite(request)
      timeout -= (Time.now - t0)
      raise MogileFS::Timeout, 'socket read timeout' if timeout < 0
      r = IO.select([sock], nil, nil, timeout)
      return sock if r && r[0]
      raise MogileFS::Timeout, 'socket read timeout'
    end

  end

end

