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
  # The URI this file will be stored to.

  attr_reader :uri

  ##
  # The key for this file.  This key won't represent a real file until you've
  # called #close.

  attr_reader :key

  ##
  # The class of this file.

  attr_reader :class

  ##
  # The big_io name in case we have file > 256M

  attr_accessor :big_io

  ##
  # Works like File.open.  Use MogileFS::MogileFS#new_file instead of this
  # method.

  def self.open(*args)
    fp = new(*args)
    fp.set_encoding(Encoding::BINARY) if fp.respond_to?(:set_encoding)

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

  def initialize(mg, fid, klass, key, dests, content_length)
    super ''
    @mg = mg
    @fid = fid
    @uri = @devid = nil
    @klass = klass
    @key = key
    @big_io = nil

    @dests = dests
    @tried = {}

    @socket = nil
  end

  ##
  # Writes an HTTP PUT request to +sock+ to upload the file and
  # returns file size if the socket finished writing
  def upload(devid, uri)
    file_size = length
    sock = Socket.mogilefs_new(uri.host, uri.port)
    sock.mogilefs_tcp_cork = true

    if @big_io
      # Don't try to run out of memory
      File.open(@big_io, "rb") do |fp|
        file_size = fp.stat.size
        fp.sync = true
        syswrite_full(sock, "PUT #{uri.request_uri} HTTP/1.0\r\n" \
                            "Content-Length: #{file_size}\r\n\r\n")
        sysrwloop(fp, sock)
      end
    else
      syswrite_full(sock, "PUT #{uri.request_uri} HTTP/1.0\r\n" \
                          "Content-Length: #{length}\r\n\r\n#{string}")
    end
    sock.mogilefs_tcp_cork = false

    line = sock.gets or
      raise EmptyResponseError, 'Unable to read response line from server'

    if line =~ %r%^HTTP/\d+\.\d+\s+(\d+)% then
      case $1.to_i
      when 200..299 then # success!
      else
        raise BadResponseError, "HTTP response status from upload: #{$1}"
      end
    else
      raise UnparseableResponseError, "Response line not understood: #{line}"
    end

    @mg.backend.create_close(:fid => @fid, :devid => devid,
                             :domain => @mg.domain, :key => @key,
                             :path => uri.to_s, :size => file_size)
    file_size
  end # def upload

  def close
    try_dests = @dests.dup
    last_err = nil

    loop do
      devid, url = try_dests.shift
      devid && url or break

      uri = URI.parse(url)
      begin
        bytes = upload(devid, uri)
        @devid, @uri = devid, uri
        return bytes
      rescue SystemCallError, Errno::ECONNREFUSED, MogileFS::Timeout,
             EmptyResponseError, BadResponseError,
             UnparseableResponseError => err
        last_err = @tried[url] = err
      end
    end

    raise last_err ? last_err : NoStorageNodesError
  end

end

