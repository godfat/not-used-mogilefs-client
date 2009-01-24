require 'mogilefs/client'
require 'mogilefs/util'

##
# MogileFS File manipulation client.

class MogileFS::MogileFS < MogileFS::Client

  include MogileFS::Util
  include MogileFS::Bigfile

  ##
  # The domain of keys for this MogileFS client.

  attr_reader :domain

  ##
  # The timeout for get_file_data.  Defaults to five seconds.

  attr_accessor :get_file_data_timeout

  ##
  # internal Regexp for matching an "HTTP 200 OK" head response
  HTTP_200_OK = %r{\AHTTP/\d+\.\d+\s+200\s+}.freeze

  ##
  # Creates a new MogileFS::MogileFS instance.  +args+ must include a key
  # :domain specifying the domain of this client.

  def initialize(args = {})
    @domain = args[:domain]

    @get_file_data_timeout = 5

    raise ArgumentError, "you must specify a domain" unless @domain

    if @backend = args[:db_backend]
      @readonly = true
    else
      super
    end
  end

  ##
  # Enumerates keys starting with +key+.

  def each_key(prefix)
    after = nil

    keys, after = list_keys prefix

    until keys.nil? or keys.empty? do
      keys.each { |k| yield k }
      keys, after = list_keys prefix, after
    end

    return nil
  end

  ##
  # Retrieves the contents of +key+.

  def get_file_data(key, &block)
    paths = get_paths key

    return nil unless paths

    paths.each do |path|
      next unless path
      case path
      when /^http:\/\// then
        begin
          sock = http_get_sock(URI.parse(path))
          return block_given? ? yield(sock) : sock.read
        rescue MogileFS::Timeout, Errno::ECONNREFUSED,
               EOFError, SystemCallError, MogileFS::InvalidResponseError
          next
        end
      else
        next unless File.exist? path
        return File.read(path)
      end
    end

    return nil
  end

  ##
  # Get the paths for +key+.

  def get_paths(key, noverify = true, zone = nil)
    opts = { :domain => @domain, :key => key,
             :noverify => noverify ? 1 : 0, :zone => zone }
    @backend.respond_to?(:_get_paths) and return @backend._get_paths(opts)
    res = @backend.get_paths(opts)
    (1..res['paths'].to_i).map { |i| res["path#{i}"] }
  end

  ##
  # Creates a new file +key+ in +klass+.  +bytes+ is currently unused.
  #
  # The +block+ operates like File.open.

  def new_file(key, klass = nil, bytes = 0, &block) # :yields: file
    raise MogileFS::ReadOnlyError if readonly?
    opts = { :domain => @domain, :key => key, :multi_dest => 1 }
    opts[:class] = klass if klass
    res = @backend.create_open(opts)

    dests = if dev_count = res['dev_count'] # multi_dest succeeded
      (1..dev_count.to_i).map do |i|
        [res["devid_#{i}"], res["path_#{i}"]]
      end
    else # single destination returned
      # 0x0040:  d0e4 4f4b 2064 6576 6964 3d31 2666 6964  ..OK.devid=1&fid
      # 0x0050:  3d33 2670 6174 683d 6874 7470 3a2f 2f31  =3&path=http://1
      # 0x0060:  3932 2e31 3638 2e31 2e37 323a 3735 3030  92.168.1.72:7500
      # 0x0070:  2f64 6576 312f 302f 3030 302f 3030 302f  /dev1/0/000/000/
      # 0x0080:  3030 3030 3030 3030 3033 2e66 6964 0d0a  0000000003.fid..

      [[res['devid'], res['path']]]
    end

    case (dests[0][1] rescue nil)
    when nil, '' then
      raise MogileFS::EmptyPathError
    when /^http:\/\// then
      MogileFS::HTTPFile.open(self, res['fid'], klass, key,
                              dests, bytes, &block)
    else
      raise MogileFS::UnsupportedPathError,
            "paths '#{dests.inspect}' returned by backend is not supported"
    end
  end

  ##
  # Copies the contents of +file+ into +key+ in class +klass+.  +file+ can be
  # either a file name or an object that responds to #read.

  def store_file(key, klass, file)
    raise MogileFS::ReadOnlyError if readonly?

    new_file key, klass do |mfp|
      if file.respond_to? :sysread then
        return sysrwloop(file, mfp)
      else
	if File.size(file) > 0x10000 # Bigass file, handle differently
	  mfp.big_io = file
	  return
	else
          return File.open(file) { |fp| sysrwloop(fp, mfp) }
        end
      end
    end
  end

  ##
  # Stores +content+ into +key+ in class +klass+.

  def store_content(key, klass, content)
    raise MogileFS::ReadOnlyError if readonly?

    new_file key, klass do |mfp|
      mfp << content
    end

    return content.length
  end

  ##
  # Removes +key+.

  def delete(key)
    raise MogileFS::ReadOnlyError if readonly?

    @backend.delete :domain => @domain, :key => key
  end

  ##
  # Sleeps +duration+.

  def sleep(duration)
    @backend.sleep :duration => duration
  end

  ##
  # Renames a key +from+ to key +to+.

  def rename(from, to)
    raise MogileFS::ReadOnlyError if readonly?

    @backend.rename :domain => @domain, :from_key => from, :to_key => to
    nil
  end

  ##
  # Returns the size of +key+.
  def size(key)
    @backend.respond_to?(:_size) and return @backend._size(domain, key)
    paths = get_paths(key) or return nil
    paths_size(paths)
  end

  def paths_size(paths)
    paths.each do |path|
      next unless path
      case path
      when /^http:\/\// then
        begin
          url = URI.parse path
          s = Socket.mogilefs_new_request(url.host, url.port,
                                   "HEAD #{url.request_uri} HTTP/1.0\r\n\r\n",
                                   @get_file_data_timeout)
          res = s.recv(4096, 0)
          if res =~ HTTP_200_OK
            head, body = res.split(/\r\n\r\n/, 2)
            if head =~ /^Content-Length:\s*(\d+)/i
              return $1.to_i
            end
          end
          next
        rescue MogileFS::Timeout, Errno::ECONNREFUSED,
               EOFError, SystemCallError
          next
        ensure
          s.close rescue nil
        end
      else
        next unless File.exist? path
        return File.size(path)
      end
    end

    nil
  end

  ##
  # Lists keys starting with +prefix+ follwing +after+ up to +limit+.  If
  # +after+ is nil the list starts at the beginning.

  def list_keys(prefix, after = nil, limit = 1000, &block)
    if @backend.respond_to?(:_list_keys)
      return @backend._list_keys(domain, prefix, after, limit, &block)
    end

    res = begin
      @backend.list_keys(:domain => domain, :prefix => prefix,
                         :after => after, :limit => limit)
    rescue MogileFS::Backend::NoneMatchError
      return nil
    end

    keys = (1..res['key_count'].to_i).map { |i| res["key_#{i}"] }
    if block_given?
      # emulate the MogileFS::Mysql interface, slowly...
      keys.each do |key|
        paths = get_paths(key) or next
        length = paths_size(paths) or next
        yield key, length, paths.size
      end
    end

    return keys, res['next_after']
  end

  protected

    # given a URI, this returns a readable socket with ready data from the
    # body of the response.
    def http_get_sock(uri)
      sock = Socket.mogilefs_new_request(uri.host, uri.port,
                                    "GET #{uri.request_uri} HTTP/1.0\r\n\r\n",
                                    @get_file_data_timeout)
      buf = sock.recv(4096, Socket::MSG_PEEK)
      head, body = buf.split(/\r\n\r\n/, 2)
      if head =~ HTTP_200_OK
        sock.recv(head.size + 4, 0)
        return sock
      end
      raise MogileFS::InvalidResponseError,
            "GET on #{uri} returned: #{head.inspect}"
    end # def http_get_sock

end

