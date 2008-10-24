require 'mogilefs'
require 'mogilefs/backend' # for the exceptions
require 'mysql'

# read-only interface that looks like MogileFS::MogileFS This provides
# direct, read-only access to any slave MySQL database to provide better
# performance and eliminate extra points of failure

class MogileFS::Mysql

  attr_accessor :domain
  attr_reader :my

  ##
  # Creates a new MogileFS::Mysql instance.  +args+ must include a key
  # :domain specifying the domain of this client.  Further arguments
  # that are specific (and passed directly) to Mysql include:
  #   :host, :user, :passwd, :db, :port, :sock, :flag
  # :reconnect is on by default and will enable the auto-reconnect
  # behavior of the underlying Mysql driver.
  # The :connect_timeout, :read_timeout, :write_timeout options
  # allow changing the various timeouts of the underlying Mysql driver
  def initialize(args = {})
    @domain = args[:domain]
    @my = Mysql.new(args[:host], args[:user], args[:passwd],
                    args[:db], args[:port], args[:sock], args[:flag])
    @my.reconnect = args[:reconnect] if args.include?(:reconnect)
    @my.options(Mysql::OPT_CONNECT_TIMEOUT,
                args[:connect_timeout] ? args[:connect_timeout] : 1)
    @my.options(Mysql::OPT_READ_TIMEOUT,
                args[:read_timeout] ? args[:read_timeout] : 1)
    @my.options(Mysql::OPT_WRITE_TIMEOUT,
                args[:write_timeout] ? args[:write_timeout] : 1)
    @last_update_device = @last_update_domain = Time.at(0)
    @cache_domain = @cache_device = nil
  end

  ##
  # Lists keys starting with +prefix+ follwing +after+ up to +limit+.  If
  # +after+ is nil the list starts at the beginning.
  def list_keys(prefix, after = '', limit = 1000, &block)
    # this code is based on server/lib/MogileFS/Worker/Query.pm
    dmid = refresh_domain[@domain] or \
      raise MogileFS::Backend::DomainNotFoundError

    # don't modify passed arguments
    limit ||= 1000
    limit = limit.to_i
    limit = 1000 if limit > 1000 || limit <= 0
    after = "#{after}"
    prefix = "#{prefix}"

    if after.length > 0 && /^#{Regexp.quote(prefix)}/ !~ after
      raise MogileFS::Backend::AfterMismatchError
    end

    raise MogileFS::Backend::InvalidCharsError if /[%\\]/ =~ prefix
    prefix.gsub!(/_/, '\_') # not sure why MogileFS::Worker::Query does this...

    sql = <<-EOS
    SELECT dkey,length,devcount FROM file
    WHERE dmid = #{dmid}
      AND dkey LIKE '#{@my.quote(prefix)}%'
      AND dkey > '#{@my.quote(after)}'
    ORDER BY dkey LIMIT #{limit}
    EOS

    keys = []
    @my.c_async_query(sql).each do |dkey,length,devcount|
      yield(dkey, length, devcount) if block_given?
      keys << dkey
    end
    return [ keys, keys.last || '']
  end

  ##
  # Returns the size of +key+.
  def size(key)
    dmid = refresh_domain[@domain] or \
      raise MogileFS::Backend::DomainNotFoundError

    sql = <<-EOS
    SELECT length FROM file
    WHERE dmid = #{dmid} AND dkey = '#{@my.quote(key)}'
    LIMIT 1
    EOS

    res = @my.c_async_query(sql).fetch_row
    return res[0].to_i if res && res[0]
    raise MogileFS::Backend::UnknownKeyError
  end

  ##
  # Get the paths for +key+.
  def get_paths(key, noverify = true, zone = nil)
    dmid = refresh_domain[@domain] or \
      raise MogileFS::Backend::DomainNotFoundError
    devices = refresh_device or raise MogileFS::Backend::NoDevicesError
    urls = []
    sql = <<-EOS
    SELECT fid FROM file
    WHERE dmid = #{dmid} AND dkey = '#{@my.quote(key)}'
    LIMIT 1
    EOS

    res = @my.c_async_query(sql).fetch_row
    res && res[0] or raise MogileFS::Backend::UnknownKeyError
    fid = res[0]
    sql = "SELECT devid FROM file_on WHERE fid = '#{@my.quote(fid)}'"
    @my.c_async_query(sql).each do |devid,|
      devinfo = devices[devid.to_i]
      port = devinfo[:http_get_port] || devinfo[:http_port] || 80
      host = zone && zone == 'alt' ? devinfo[:altip] : devinfo[:hostip]
      nfid = '%010u' % fid
      b, mmm, ttt = /(\d)(\d{3})(\d{3})(?:\d{3})/.match(nfid)[1..3]
      uri = "/dev#{devid}/#{b}/#{mmm}/#{ttt}/#{nfid}.fid"
      urls << "http://#{host}:#{port}#{uri}"
    end
    urls
  end

  private

    unless defined? GET_DEVICES
      GET_DEVICES = <<-EOS
        SELECT d.devid, h.hostip, h.altip, h.http_port, h.http_get_port
        FROM device d
          LEFT JOIN host h ON d.hostid = h.hostid
        WHERE d.status IN ('alive','readonly','drain');
      EOS
      GET_DEVICES.freeze
    end

    def refresh_device(force = false)
      return @cache_device if ! force && ((Time.now - @last_update_device) < 60)
      tmp = {}
      res = @my.c_async_query(GET_DEVICES)
      res.each do |devid, hostip, altip, http_port, http_get_port|
        tmp[devid.to_i] = {
          :hostip => hostip.freeze,
          :altip => altip.freeze,
          :http_port => http_port ? http_port.to_i : nil,
          :http_get_port => http_get_port ? http_get_port.to_i : nil,
        }.freeze
      end
      @last_update_device = Time.now
      @cache_device = tmp.freeze
    end

    def refresh_domain(force = false)
      return @cache_domain if ! force && ((Time.now - @last_update_domain) < 5)
      tmp = {}
      res = @my.c_async_query('SELECT dmid,namespace FROM domain')
      res.each { |dmid,namespace| tmp[namespace] = dmid.to_i }
      @last_update_domain = Time.now
      @cache_domain = tmp.freeze
    end

end
