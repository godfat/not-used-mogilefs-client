require 'mogilefs'
require 'mogilefs/backend' # for the exceptions
require 'mysql'

# read-only interface that looks like MogileFS::MogileFS This provides
# direct, read-only access to any slave MySQL database to provide better
# performance and eliminate extra points of failure

class MogileFS::Mysql

  attr_accessor :domain

  def initialize(param = {})
    @domain = param[:domain]
    @my = Mysql.new(param[:host], param[:user], param[:passwd],
                    param[:db], param[:port], param[:sock], param[:flag])
    @my.reconnect = true
    @last_update_device = @last_update_domain = Time.at(0)
    @cache_domain = @cache_device = nil
  end

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
    prefix.gsub!(/_/, '\_')
    prefix << '%'

    st = @my.prepare <<-EOS
      SELECT dkey,length,devcount FROM file
      WHERE dmid = ? AND dkey LIKE ? AND dkey > ?
      ORDER BY dkey LIMIT #{limit}
    EOS
    st.execute(dmid, prefix, after)

    keys = []
    st.each do |dkey,length,devcount|
      yield(dkey, length, devcount) if block_given?
      keys << dkey
    end
    return [ keys, keys.last || '']
    ensure
      st.close rescue nil
  end

  def size(key)
    dmid = refresh_domain[@domain] or \
      raise MogileFS::Backend::DomainNotFoundError
    st = @my.prepare('SELECT length FROM file ' \
                     'WHERE dmid = ? AND dkey = ? LIMIT 1')
    st.execute(dmid, key)
    unless res = st.fetch
      raise MogileFS::Backend::UnknownKeyError
      return
    end
    return res[0]
    ensure
      st.close rescue nil
  end

  def get_paths(key, noverify = true, zone = nil)
    dmid = refresh_domain[@domain] or \
      raise MogileFS::Backend::DomainNotFoundError
    devices = refresh_device or raise MogileFS::Backend::NoDevicesError
    urls = []
    st = @my.prepare('SELECT fid FROM file WHERE dmid = ? AND dkey = ? LIMIT 1')
    st.execute(dmid, key)
    unless res = st.fetch
      raise MogileFS::Backend::UnknownKeyError
      return
    end
    if fid = res[0]
      st.prepare('SELECT devid FROM file_on WHERE fid = ?')
      st.execute(fid)
      st.each do |devid,|
        devinfo = devices[devid.to_i]
        port = devinfo[:http_get_port] || devinfo[:http_port] || 80
        host = zone && zone == 'alt' ? devinfo[:altip] : devinfo[:hostip]
        nfid = '%010u' % fid
        b, mmm, ttt = /(\d)(\d{3})(\d{3})(?:\d{3})/.match(nfid)[1..3]
        uri = "/dev#{devid}/#{b}/#{mmm}/#{ttt}/#{nfid}.fid"
        urls << "http://#{host}:#{port}#{uri}"
      end
    else
      urls = nil
    end
    return urls
    ensure
      st.close rescue nil
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
      @my.query(GET_DEVICES) do |res|
        res.each do |devid, hostip, altip, http_port, http_get_port|
          tmp[devid.to_i] = {
            :hostip => hostip.freeze,
            :altip => altip.freeze,
            :http_port => http_port ? http_port.to_i : nil,
            :http_get_port => http_get_port ? http_get_port.to_i : nil,
          }.freeze
        end
      end
      @last_update_device = Time.now
      @cache_device = tmp.freeze
    end

    def refresh_domain(force = false)
      return @cache_domain if ! force && ((Time.now - @last_update_domain) < 5)
      tmp = {}
      @my.query('SELECT dmid,namespace FROM domain') do |res|
        res.each { |dmid,namespace| tmp[namespace] = dmid.to_i }
      end
      @last_update_domain = Time.now
      @cache_domain = tmp.freeze
    end

end
