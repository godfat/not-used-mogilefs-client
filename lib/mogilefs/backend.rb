require 'socket'
require 'thread'
require 'mogilefs'

##
# MogileFS::Backend communicates with the MogileFS trackers.

class MogileFS::Backend

  ##
  # Adds MogileFS commands +names+.

  def self.add_command(*names)
    names.each do |name|
      define_method name do |*args|
        do_request name, args.first || {}
      end
    end
  end

  # this converts an error code from a mogilefsd tracker to an exception:
  #
  # Examples of some exceptions that get created:
  #   class AfterMismatchError < MogileFS::Error; end
  #   class DomainNotFoundError < MogileFS::Error; end
  #   class InvalidCharsError < MogileFS::Error; end
  def error(err_snake)
    err_camel = err_snake.gsub(/(?:^|_)([a-z])/) { $1.upcase } << 'Error'
    unless self.class.const_defined?(err_camel)
      self.class.class_eval("class #{err_camel} < MogileFS::Error; end")
    end
    self.class.const_get(err_camel)
  end

  ##
  # The last error
  #--
  # TODO Use Exceptions

  attr_reader :lasterr

  ##
  # The string attached to the last error
  #--
  # TODO Use Exceptions

  attr_reader :lasterrstr

  ##
  # Creates a new MogileFS::Backend.
  #
  # :hosts is a required argument and must be an Array containing one or more
  # 'hostname:port' pairs as Strings.
  #
  # :timeout adjusts the request timeout before an error is returned.

  def initialize(args)
    @hosts = args[:hosts]
    raise ArgumentError, "must specify at least one host" unless @hosts
    raise ArgumentError, "must specify at least one host" if @hosts.empty?
    unless @hosts == @hosts.select { |h| h =~ /:\d+$/ } then
      raise ArgumentError, ":hosts must be in 'host:port' form"
    end

    @mutex = Mutex.new
    @timeout = args[:timeout] || 3
    @socket = nil
    @lasterr = nil
    @lasterrstr = nil

    @dead = {}
  end

  ##
  # Closes this backend's socket.

  def shutdown
    @socket.close unless @socket.nil? or @socket.closed?
    @socket = nil
  end

  # MogileFS::MogileFS commands

  add_command :create_open
  add_command :create_close
  add_command :get_paths
  add_command :delete
  add_command :sleep
  add_command :rename
  add_command :list_keys

  # MogileFS::Backend commands
  
  add_command :get_hosts
  add_command :get_devices
  add_command :list_fids
  add_command :stats
  add_command :get_domains
  add_command :create_domain
  add_command :delete_domain
  add_command :create_class
  add_command :update_class
  add_command :delete_class
  add_command :create_host
  add_command :update_host
  add_command :delete_host
  add_command :set_state

  private unless defined? $TESTING

  ##
  # Returns a new TCPSocket connected to +port+ on +host+.

  def connect_to(host, port)
    return TCPSocket.new(host, port)
  end

  ##
  # Performs the +cmd+ request with +args+.

  def do_request(cmd, args)
    @mutex.synchronize do
      request = make_request cmd, args

      begin
        bytes_sent = socket.send request, 0
      rescue SystemCallError
        @socket = nil
        raise "couldn't connect to mogilefsd backend"
      end

      unless bytes_sent == request.length then
        raise "request truncated (sent #{bytes_sent} expected #{request.length})"
      end

      readable?

      return parse_response(socket.gets)
    end
  end

  ##
  # Makes a new request string for +cmd+ and +args+.

  def make_request(cmd, args)
    return "#{cmd} #{url_encode args}\r\n"
  end

  ##
  # Turns the +line+ response from the server into a Hash of options, an
  # error, or raises, as appropriate.

  def parse_response(line)
    if line =~ /^ERR\s+(\w+)\s*(.*)/ then
      @lasterr = $1
      @lasterrstr = $2 ? url_unescape($2) : nil
      return nil
    end

    return url_decode($1) if line =~ /^OK\s+\d*\s*(\S*)/

    raise "Invalid response from server: #{line.inspect}"
  end

  ##
  # Raises if the socket does not become readable in +@timeout+ seconds.

  def readable?
    found = select [socket], nil, nil, @timeout
    if found.nil? or found.empty? then
      peer = (@socket ? "#{@socket.peeraddr[3]}:#{@socket.peeraddr[1]} " : nil)
      socket.close  # we DO NOT want the response we timed out waiting for, to crop up later on, on the same socket, intersperesed with a subsequent request! so, we close the socket if it times out like this
      raise MogileFS::UnreadableSocketError, "#{peer}never became readable"
    end
    return true
  end

  ##
  # Returns a socket connected to a MogileFS tracker.

  def socket
    return @socket if @socket and not @socket.closed?

    now = Time.now

    @hosts.sort_by { rand(3) - 1 }.each do |host|
      next if @dead.include? host and @dead[host] > now - 5

      begin
        @socket = connect_to(*host.split(':'))
      rescue SystemCallError
        @dead[host] = now
        next
      end

      return @socket
    end

    raise "couldn't connect to mogilefsd backend"
  end

  ##
  # Turns a url params string into a Hash.

  def url_decode(str)
    pairs = str.split('&').map do |pair|
      pair.split('=', 2).map { |v| url_unescape v }
    end

    return Hash[*pairs.flatten]
  end

  ##
  # Turns a Hash (or Array of pairs) into a url params string.

  def url_encode(params)
    return params.map do |k,v|
      "#{url_escape k.to_s}=#{url_escape v.to_s}"
    end.join("&")
  end

  ##
  # Escapes naughty URL characters.

  def url_escape(str)
    return str.gsub(/([^\w\,\-.\/\\\: ])/) { "%%%02x" % $1[0] }.tr(' ', '+')
  end

  ##
  # Unescapes naughty URL characters.

  def url_unescape(str)
    return str.gsub(/%([a-f0-9][a-f0-9])/i) { [$1.to_i(16)].pack 'C' }.tr('+', ' ')
  end

end

