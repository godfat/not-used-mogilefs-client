require 'stringio'
require 'uri'
require 'mogilefs/backend'
require 'mogilefs/util'

##
# HTTPFile wraps up the new file operations for storing files onto an HTTP
# storage node.
#
# You really don't want to create an HTTPFile by hand.  Instead you want to
# create a new file using MogileFS::MogileFS.new_file.
#
#--
# TODO dup'd content in MogileFS::NFSFile

class MogileFS::HTTPFile < StringIO
  include MogileFS::Util

  class EmptyResponseError < MogileFS::Error; end
  class BadResponseError < MogileFS::Error; end
  class UnparseableResponseError < MogileFS::Error; end
  class NoStorageNodesError < MogileFS::Error
    def message; 'Unable to open socket to storage node'; end
  end

  ##
  # The path this file will be stored to.

  attr_reader :path

  ##
  # The key for this file.  This key won't represent a real file until you've
  # called #close.

  attr_reader :key

  ##
  # The class of this file.

  attr_reader :class

  ##
  # The bigfile name in case we have file > 256M

  attr_accessor :bigfile

  ##
  # Works like File.open.  Use MogileFS::MogileFS#new_file instead of this
  # method.

  def self.open(*args)
    fp = new(*args)

    return fp unless block_given?

    begin
      yield fp
    ensure
      fp.close
    end
  end

  ##
  # Creates a new HTTPFile with MogileFS-specific data.  Use
  # MogileFS::MogileFS#new_file instead of this method.

  def initialize(mg, fid, path, devid, klass, key, dests, content_length)
    super ''
    @mg = mg
    @fid = fid
    @path = path
    @devid = devid
    @klass = klass
    @key = key
    @bigfile = nil

    @dests = dests.map { |(_,u)| URI.parse u }
    @tried = {}

    @socket = nil
  end

  ##
  # Closes the file handle and marks it as closed in MogileFS.

  def close
    connect_socket

    file_size = nil
    if @bigfile
      # Don't try to run out of memory
      File.open(@bigfile) do |fp|
        file_size = fp.stat.size
        @socket.mogilefs_tcp_cork = fp.sync = true
        @socket.send("PUT #{@path.request_uri} HTTP/1.0\r\n" \
                     "Content-Length: #{file_size}\r\n\r\n", 0)
        sysrwloop(fp, @socket)
        @socket.mogilefs_tcp_cork = false
      end
    else
      @socket.send("PUT #{@path.request_uri} HTTP/1.0\r\n" \
                   "Content-Length: #{length}\r\n\r\n#{string}", 0)
    end

    if connected? then
      line = @socket.gets
      if line.nil?
        raise EmptyResponseError, 'Unable to read response line from server'
      end

      if line =~ %r%^HTTP/\d+\.\d+\s+(\d+)% then
        status = Integer $1
        case status
        when 200..299 then # success!
        else
          raise BadResponseError, "HTTP response status from upload: #{status}"
        end
      else
        raise InvalidResponseError, "Response line not understood: #{line}"
      end

      @socket.close
    end

    @mg.backend.create_close(:fid => @fid, :devid => @devid,
                             :domain => @mg.domain, :key => @key,
                             :path => @path, :size => length)
    return file_size if @bigfile
    return nil
  end

  private

  def connected?
    return !(@socket.nil? or @socket.closed?)
  end

  def connect_socket
    return @socket if connected?

    next_path

    if @path.nil? then
      @tried.clear
      next_path
      raise NoStorageNodesError if @path.nil?
    end

    @socket = Socket.mogilefs_new @path.host, @path.port
  end

  def next_path
    @path = nil
    @dests.each do |dest|
      unless @tried.include? dest then
        @path = dest
        return
      end
    end
  end

end

