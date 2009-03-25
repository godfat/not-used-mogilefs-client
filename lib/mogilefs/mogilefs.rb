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

    nil
  end

  ##
  # Retrieves the contents of +key+.

  def get_file_data(key, &block)
    paths = get_paths(key) or return nil
    paths.each do |path|
      begin
        sock = http_read_sock(URI.parse(path))
        begin
          return yield(sock) if block_given?
          return sysread_full(sock, sock.mogilefs_size, @get_file_data_timeout)
        ensure
          sock.close rescue nil
        end
      rescue MogileFS::Timeout, MogileFS::InvalidResponseError,
             Errno::ECONNREFUSED, EOFError, SystemCallError
      end
    end
    nil
  end

  ##
  # Get the paths for +key+.

  def get_paths(key, noverify = true, zone = nil)
    opts = { :domain => @domain, :key => key,
             :noverify => noverify ? 1 : 0, :zone => zone }
    @backend.respond_to?(:_get_paths) and return @backend._get_paths(opts)
    res = @backend.get_paths(opts)
    (1..res['paths'].to_i).map { |i| res["path#{i}"] }.compact
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
          return File.open(file, "rb") { |fp| sysrwloop(fp, mfp) }
        end
      end
    end
  end

  ##
  # Stores +content+ into +key+ in class +klass+.

  def store_content(key, klass, content)
    raise MogileFS::ReadOnlyError if readonly?

    new_file key, klass do |mfp|
      if content.is_a?(MogileFS::Util::StoreContent)
        mfp.streaming_io = content
      else
        mfp << content
      end
    end

    content.length
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
      begin
        return http_read_sock(URI.parse(path), "HEAD").mogilefs_size
      rescue MogileFS::InvalidResponseError, MogileFS::Timeout,
             Errno::ECONNREFUSED, EOFError, SystemCallError => err
        next
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

    [ keys, res['next_after'] ]
  end

  protected

    # given a URI, this returns a readable socket with ready data from the
    # body of the response.
    def http_read_sock(uri, http_method = "GET")
      sock = Socket.mogilefs_new_request(uri.host, uri.port,
                    "#{http_method} #{uri.request_uri} HTTP/1.0\r\n\r\n",
                    @get_file_data_timeout)
      buf = sock.recv_nonblock(4096, Socket::MSG_PEEK)
      head, body = buf.split(/\r\n\r\n/, 2)

      # we're dealing with a seriously slow/stupid HTTP server if we can't
      # get the header in a single read(2) syscall.
      if head =~ %r{\AHTTP/\d+\.\d+\s+200\s*} &&
         head =~ %r{^Content-Length:\s*(\d+)}i
        sock.mogilefs_size = $1.to_i
        case http_method
        when "HEAD" then sock.close
        when "GET" then sock.recv(head.size + 4, 0)
        end
        return sock
      end
      sock.close rescue nil
      raise MogileFS::InvalidResponseError,
            "#{http_method} on #{uri} returned: #{head.inspect}"
    end # def http_read_sock

end
