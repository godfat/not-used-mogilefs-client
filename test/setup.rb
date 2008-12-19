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

  def error(err_snake)
    err_camel = err_snake.gsub(/(?:^|_)([a-z])/) { $1.upcase } << 'Error'
    unless MogileFS::Backend.const_defined?(err_camel)
      MogileFS::Backend.class_eval("class #{err_camel} < MogileFS::Error; end")
    end
    MogileFS::Backend.const_get(err_camel)
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
        raise error(@lasterr), @lasterrstr
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
    @sync = do_sync
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

  def read(bytes = nil)
    @read_s.read bytes
  end

  def sysread(bytes, buf = '')
    @read_s.sysread bytes, buf
  end

  def recv_nonblock(bytes, flags = 0)
    ret = @read_s.sysread(bytes)
    # Ruby doesn't expose pread(2)
    if (flags & Socket::MSG_PEEK) != 0
      if @read_s.respond_to?(:sysseek)
        @read_s.sysseek(-ret.size, IO::SEEK_CUR)
      else
        @read_s.seek(-ret.size, IO::SEEK_CUR)
      end
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

# for our mock results
class Array
  alias_method :fetch_row, :shift
end

class FakeMysql
  attr_reader :expect
  TBL_DEVICES = [
    # devid, hostip,    altip,         http_port, http_get_port
    [ 1,    '10.0.0.1', '192.168.0.1', 7500,      7600 ],
    [ 2,    '10.0.0.2', '192.168.0.2', 7500,      7600 ],
    [ 3,    '10.0.0.3', nil,           7500,      nil ],
    [ 4,    '10.0.0.4', nil,           7500,      nil ],
  ]
  TBL_DOMAINS = [
    # dmid, namespace
    [ 1, 'test' ],
    [ 2, 'foo' ],
  ]

  def initialize
    @expect = []
  end

  def quote(str)
    str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
  end

  def query(sql = '')
    case sql
    when MogileFS::Mysql::GET_DEVICES then TBL_DEVICES
    when MogileFS::Mysql::GET_DOMAINS then TBL_DOMAINS
    else
      @expect.shift
    end
  end

end


