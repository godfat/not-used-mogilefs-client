require 'mogilefs'
require 'mogilefs/util'

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

  BACKEND_ERRORS = {}

  # this converts an error code from a mogilefsd tracker to an exception:
  #
  # Examples of some exceptions that get created:
  #   class AfterMismatchError < MogileFS::Error; end
  #   class DomainNotFoundError < MogileFS::Error; end
  #   class InvalidCharsError < MogileFS::Error; end
  def self.add_error(err_snake)
    err_camel = err_snake.gsub(/(?:^|_)([a-z])/) { $1.upcase } << 'Error'
    unless self.const_defined?(err_camel)
      self.class_eval("class #{err_camel} < MogileFS::Error; end")
    end
    BACKEND_ERRORS[err_snake] = self.const_get(err_camel)
  end

  ##
  # The last error

  attr_reader :lasterr

  ##
  # The string attached to the last error

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
    if @socket
      @socket.close rescue nil # ignore errors
      @socket = nil
    end
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

  # Errors copied from MogileFS/Worker/Query.pm
  add_error 'dup'
  add_error 'after_mismatch'
  add_error 'bad_params'
  add_error 'class_exists'
  add_error 'class_has_files'
  add_error 'class_not_found'
  add_error 'db'
  add_error 'domain_has_files'
  add_error 'domain_exists'
  add_error 'domain_not_empty'
  add_error 'domain_not_found'
  add_error 'failure'
  add_error 'host_exists'
  add_error 'host_mismatch'
  add_error 'host_not_empty'
  add_error 'host_not_found'
  add_error 'invalid_chars'
  add_error 'invalid_checker_level'
  add_error 'invalid_mindevcount'
  add_error 'key_exists'
  add_error 'no_class'
  add_error 'no_devices'
  add_error 'no_domain'
  add_error 'no_host'
  add_error 'no_ip'
  add_error 'no_key'
  add_error 'no_port'
  add_error 'none_match'
  add_error 'plugin_aborted'
  add_error 'state_too_high'
  add_error 'unknown_command'
  add_error 'unknown_host'
  add_error 'unknown_key'
  add_error 'unknown_state'
  add_error 'unreg_domain'

  private unless defined? $TESTING

  ##
  # Returns a new Socket (TCP) connected to +port+ on +host+.

  def connect_to(host, port)
    Socket.mogilefs_new(host, port, @timeout)
  end

  ##
  # Performs the +cmd+ request with +args+.

  def do_request(cmd, args)
    @mutex.synchronize do
      request = make_request cmd, args

      begin
        bytes_sent = socket.send request, 0
      rescue SystemCallError
        shutdown
        raise MogileFS::UnreachableBackendError
      end

      unless bytes_sent == request.length then
        raise MogileFS::RequestTruncatedError,
          "request truncated (sent #{bytes_sent} expected #{request.length})"
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

  # this converts an error code from a mogilefsd tracker to an exception
  # Most of these exceptions should already be defined, but since the
  # MogileFS server code is liable to change and we may not always be
  # able to keep up with the changes
  def error(err_snake)
    BACKEND_ERRORS[err_snake] || self.class.add_error(err_snake)
  end

  ##
  # Turns the +line+ response from the server into a Hash of options, an
  # error, or raises, as appropriate.

  def parse_response(line)
    if line =~ /^ERR\s+(\w+)\s*(.*)/ then
      @lasterr = $1
      @lasterrstr = $2 ? url_unescape($2) : nil
      raise error(@lasterr)
      return nil
    end

    return url_decode($1) if line =~ /^OK\s+\d*\s*(\S*)/

    raise MogileFS::InvalidResponseError,
          "Invalid response from server: #{line.inspect}"
  end

  ##
  # Raises if the socket does not become readable in +@timeout+ seconds.

  def readable?
    timeleft = @timeout
    peer = nil
    loop do
      t0 = Time.now
      found = select [socket], nil, nil, timeleft
      return true if found && found[0]
      timeleft -= (Time.now - t0)

      if timeleft < 0
        peer = @socket ? "#{@socket.mogilefs_peername} " : nil

        # we DO NOT want the response we timed out waiting for, to crop up later
        # on, on the same socket, intersperesed with a subsequent request! so,
        # we close the socket if it times out like this
        shutdown
        raise MogileFS::UnreadableSocketError, "#{peer}never became readable"
        break
      end
      shutdown
    end
    false
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
      rescue SystemCallError, MogileFS::Timeout
        @dead[host] = now
        next
      end

      return @socket
    end

    raise MogileFS::UnreachableBackendError
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

