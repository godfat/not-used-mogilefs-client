require 'test/unit'

require 'fileutils'
require 'tmpdir'
require 'stringio'

require 'rubygems'
require 'test/zentest_assertions'

$TESTING = true

require 'mogilefs'

class FakeBackend

  attr_reader :lasterr, :lasterrstr

  def initialize
    @responses = Hash.new { |h,k| h[k] = [] }
    @lasterr = nil
    @lasterrstr = nil
  end

  def method_missing(meth, *args)
    meth = meth.to_s
    if meth =~ /(.*)=$/ then
      @responses[$1] << args.first
    else
      response = @responses[meth].shift
      case response
      when Array then
        @lasterr = response.first
        @lasterrstr = response.last
        return nil
      end
      return response
    end
  end

end

class FakeSocket

  attr_reader :read_s
  attr_reader :write_s
  attr_reader :sync

  def initialize(read = '', write = StringIO.new)
    @read_s = read.class.method_defined?(:sysread) ? read : StringIO.new(read)
    @write_s = write
    @closed = false
    @sync = false
  end

  def sync=(do_sync)
    @write_s.sync = do_sync
    @read_s.sync = do_sync
  end

  def closed?
    @closed
  end

  def close
    @closed = true
    return nil
  end

  def gets
    @read_s.gets
  end

  def peeraddr
    ['AF_INET', 6001, 'localhost', '127.0.0.1']
  end

  def read(bytes)
    @read_s.read bytes
  end

  def sysread(bytes, buf = '')
    @read_s.sysread bytes, buf
  end

  def recv_nonblock(bytes, flags = 0)
    ret = @read_s.sysread(bytes)
    # Ruby doesn't expose pread(2)
    if (flags & Socket::MSG_PEEK) != 0
      @read_s.sysseek(-ret.size, IO::SEEK_CUR)
    end
    ret
  end
  alias_method :recv, :recv_nonblock

  def write(data)
    @write_s.write data
  end

  def syswrite(data)
    @write_s.syswrite data
  end

end

class MogileFS::Client
  attr_writer :readonly
end

class TCPSocket

  class << self

    attr_accessor :connections
    attr_accessor :sockets

    alias old_new new

    def new(host, port)
      raise Errno::ECONNREFUSED if @sockets.empty?
      @connections << [host, port]
      @sockets.pop
    end

    alias open new

  end

end

class TestMogileFS < Test::Unit::TestCase

  undef_method :default_test

  def setup
    @tempdir = File.join Dir.tmpdir, "test_mogilefs_#{$$}"
    @root = File.join @tempdir, 'root'
    FileUtils.mkdir_p @root

    @client = @klass.new :hosts => ['kaa:6001'], :domain => 'test',
                                  :root => @root
    @backend = FakeBackend.new
    @client.instance_variable_set '@backend', @backend

    TCPSocket.sockets = []
    TCPSocket.connections = []
  end

  def teardown
    FileUtils.rm_rf @tempdir
  end

end

