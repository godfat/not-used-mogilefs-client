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
    io_rd.flush rescue nil # flush may be needed for sockets/pipes, be safe
    io_wr.flush
    io_rd.sync = io_wr.sync = true
    loop do
      b = begin
        io_rd.sysread(CHUNK_SIZE, buf)
      rescue Errno::EAGAIN, Errno::EINTR
        IO.select([io_rd], nil, nil, nil)
        retry
      rescue EOFError
        break
      end
      b = filter.call(b) if filter
      copied += syswrite_full(io_wr, b)
    end

    # filter must take nil as a possible argument to indicate EOF
    if filter
      b = filter.call(nil)
      copied += syswrite_full(io_wr, b) if b && b.length > 0
    end
    copied
  end # sysrwloop

  # writes the contents of buf to io_wr in full w/o blocking
  def syswrite_full(io_wr, buf, timeout = nil)
    written = 0
    loop do
      begin
        w = io_wr.syswrite(buf)
        written += w
        return written if w == buf.size
        buf = buf[w..-1]

        # a short syswrite means the next syswrite will likely block
        # inside the interpreter.  so force an IO.select on it so we can
        # timeout there if one was specified
        raise Errno::EAGAIN if timeout
      rescue Errno::EAGAIN, Errno::EINTR
        t0 = Time.now if timeout
        IO.select(nil, [io_wr], nil, timeout)
        if timeout && ((timeout -= (Time.now - t0)) < 0)
          raise MogileFS::Timeout, 'syswrite_full timeout'
        end
      end
    end
    # should never get here
  end

  def sysread_full(io_rd, size, timeout = nil, full_timeout = false)
    tmp = [] # avoid expensive string concatenation with every loop iteration
    reader = io_rd.method(timeout ? :read_nonblock : :sysread)
    begin
      while size > 0
        tmp << reader.call(size)
        size -= tmp.last.size
      end
    rescue Errno::EAGAIN, Errno::EINTR
      t0 = Time.now
      r = IO.select([ io_rd ], nil, nil, timeout)
      if timeout
        timeout -= (Time.now - t0) if full_timeout
        if !(r && r[0]) || timeout < 0
          raise MogileFS::Timeout, 'sysread_full timeout'
        end
      end
      retry
    rescue EOFError
    end
    tmp.join('')
  end

  class StoreContent < Proc
    def initialize(total_size, &writer_proc)
      @total_size = total_size
      super(&writer_proc)
    end
    def length
      @total_size
    end
  end

end

require 'timeout'
##
# Timeout error class.  Subclassing it from Timeout::Error is the only
# reason we require the 'timeout' module, otherwise that module is
# broken and worthless to us.
class MogileFS::Timeout < Timeout::Error; end

class Socket
  attr_accessor :mogilefs_addr, :mogilefs_connected, :mogilefs_size

  TCP_CORK = 3 if ! defined?(TCP_CORK) && RUBY_PLATFORM =~ /linux/

  def mogilefs_tcp_cork=(set)
    if defined?(TCP_CORK)
      self.setsockopt(SOL_TCP, TCP_CORK, set ? 1 : 0) rescue nil
    end
    set
  end

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
      if defined?(Socket::TCP_NODELAY)
        sock.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
      end
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

    include MogileFS::Util

    # Makes a request on a new TCP Socket and returns with a readble socket
    # within the given timeout.
    # This raises MogileFS::Timeout if timeout expires
    def mogilefs_new_request(host, port, request, timeout = 5.0)
      t0 = Time.now
      sock = mogilefs_new(host, port, timeout)
      syswrite_full(sock, request, timeout)
      timeout -= (Time.now - t0)
      if timeout < 0
        sock.close rescue nil
        raise MogileFS::Timeout, 'socket read timeout'
      end
      r = IO.select([sock], nil, nil, timeout)
      return sock if r && r[0]

      sock.close rescue nil
      raise MogileFS::Timeout, 'socket read timeout'
    end

  end

end

