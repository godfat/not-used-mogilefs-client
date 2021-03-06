STDIN.sync = STDOUT.sync = STDERR.sync = true
require 'test/unit'
require 'tempfile'
require 'fileutils'
require 'tmpdir'
require 'stringio'

trap('CHLD') { Process.waitpid(-1, Process::WNOHANG) rescue nil }

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

class MogileFS::Client
  attr_writer :readonly
end

require 'socket'
class TempServer
  attr_reader :port, :pid

  def self.destroy_all!
    ObjectSpace.each_object(TempServer) { |t| t.destroy! }
  end

  at_exit { TempServer.destroy_all! }

  def initialize(server_proc, port = nil)
    @pid = @sock = nil
    @port = port
    retries = 10
    begin
      @port ||= 1024 + rand(32768 - 1024)
      @sock = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
      @sock.bind(Socket.pack_sockaddr_in(@port.to_i, '127.0.0.1'))
      @sock.listen(5)
    rescue Errno::EADDRINUSE, Errno::EACCES => err
      @sock.close rescue nil
      @port = nil
      retry if port.nil? && (retries -= 1) > 0
      warn "retries failed: #{retries} port=#{port.inspect}"
      raise err
    end
    @pid = fork { server_proc.call(@sock, @port) }
    @sock.close rescue nil
  end

  def destroy!
    @sock.close rescue nil
    Process.kill('KILL', @pid) rescue nil
  end

end

class TestMogileFS < Test::Unit::TestCase

  undef_method :default_test if method_defined?(:default_test)

  def setup
    @client = @klass.new :hosts => ['kaa:6001'], :domain => 'test'
    @backend = FakeBackend.new
    @client.instance_variable_set '@backend', @backend
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


