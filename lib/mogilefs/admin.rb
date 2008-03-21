require 'mogilefs/client'

##
# A MogileFS Administration Client

class MogileFS::Admin < MogileFS::Client

  ##
  # Enumerates fids using #list_fids.

  def each_fid
    low = 0
    high = nil

    max = get_stats('fids')['fids']['max']

    0.step max, 100 do |high|
      fids = list_fids low, high
      fids.each { |fid| yield fid }
      low = high + 1
    end
  end

  ##
  # Returns an Array of host status Hashes.  If +hostid+ is given only that
  # host is returned.
  #
  #   admin.get_hosts 1
  #
  # Returns:
  #
  #   [{"status"=>"alive",
  #     "http_get_port"=>"",
  #     "http_port"=>"",
  #     "hostid"=>"1",
  #     "hostip"=>"",
  #     "hostname"=>"rur-1",
  #     "remoteroot"=>"/mnt/mogilefs/rur-1",
  #     "altip"=>"",
  #     "altmask"=>""}]

  def get_hosts(hostid = nil)
    args = hostid ? { :hostid => hostid } : {}
    res = @backend.get_hosts args
    return clean('hosts', 'host', res)
  end

  ##
  # Returns an Array of device status Hashes.  If devid is given only that
  # device is returned.
  #
  #   admin.get_devices 1
  #
  # Returns:
  #
  #   [{"status"=>"alive",
  #     "mb_asof"=>"",
  #     "mb_free"=>"0",
  #     "devid"=>"1",
  #     "hostid"=>"1",
  #     "mb_used"=>"",
  #     "mb_total"=>""}]

  def get_devices(devid = nil)
    args = devid ? { :devid => devid } : {}
    res = @backend.get_devices args
    return clean('devices', 'dev', res)
  end

  ##
  # Returns an Array of fid Hashes from +from_fid+ to +to_fid+.
  #
  #   admin.list_fids 0, 100
  #
  # Returns:
  #
  #   [{"fid"=>"99",
  #     "class"=>"normal",
  #     "domain"=>"test",
  #     "devcount"=>"2",
  #     "length"=>"4",
  #     "key"=>"file_key"},
  #    {"fid"=>"82",
  #     "class"=>"normal",
  #     "devcount"=>"2",
  #     "domain"=>"test",
  #     "length"=>"9",
  #     "key"=>"new_new_key"}]

  def list_fids(from_fid, to_fid)
    res = @backend.list_fids :from => from_fid, :to => to_fid
    return clean('fid_count', 'fid_', res)
  end

  ##
  # Returns a statistics structure representing the state of mogilefs.
  #
  #   admin.get_stats
  #
  # Returns:
  #
  #   {"fids"=>{"max"=>"99", "count"=>"2"},
  #    "device"=>
  #     [{"status"=>"alive", "files"=>"2", "id"=>"1", "host"=>"rur-1"},
  #      {"status"=>"alive", "files"=>"2", "id"=>"2", "host"=>"rur-2"}],
  #    "replication"=>
  #     [{"files"=>"2", "class"=>"normal", "devcount"=>"2", "domain"=>"test"}],
  #    "file"=>[{"files"=>"2", "class"=>"normal", "domain"=>"test"}]}

  def get_stats(type = 'all')
    res = @backend.stats type => 1
    stats = {}

    stats['device'] = clean 'devicescount', 'devices', res, false
    stats['file'] = clean 'filescount', 'files', res, false
    stats['replication'] = clean 'replicationcount', 'replication', res, false

    if res['fidmax'] or res['fidcount'] then
      stats['fids'] = {
        'max' => res['fidmax'].to_i,
        'count' => res['fidcount'].to_i
      }
    end

    stats.delete 'device' if stats['device'].empty?
    stats.delete 'file' if stats['file'].empty?
    stats.delete 'replication' if stats['replication'].empty?

    return stats
  end

  ##
  # Returns the domains present in the mogilefs.
  #
  #   admin.get_domains
  #
  # Returns:
  #
  #   {"test"=>{"normal"=>3, "default"=>2}}

  def get_domains
    res = @backend.get_domains

    domains = {}
    (1..res['domains'].to_i).each do |i|
      domain = clean "domain#{i}classes", "domain#{i}class", res, false
      domain = domain.map { |d| [d.values.first, d.values.last.to_i] }
      domains[res["domain#{i}"]] = Hash[*domain.flatten]
    end

    return domains
  end

  ##
  # Creates a new domain named +domain+.  Returns nil if creation failed.

  def create_domain(domain)
    raise 'readonly mogilefs' if readonly?
    res = @backend.create_domain :domain => domain
    return res['domain'] unless res.nil?
  end

  ##
  # Deletes +domain+.  Returns true if successful, false if not.

  def delete_domain(domain)
    raise 'readonly mogilefs' if readonly?
    res = @backend.delete_domain :domain => domain
    return !res.nil?
  end

  ##
  # Creates a new class in +domain+ named +klass+ with files replicated to
  # +mindevcount+ devices.  Returns nil on failure.

  def create_class(domain, klass, mindevcount)
    return modify_class(domain, klass, mindevcount, :create)
  end

  ##
  # Updates class +klass+ in +domain+ to be replicated to +mindevcount+
  # devices.  Returns nil on failure.

  def update_class(domain, klass, mindevcount)
    return modify_class(domain, klass, mindevcount, :update)
  end

  ##
  # Removes class +klass+ from +domain+.  Returns true if successful, false if
  # not.

  def delete_class(domain, klass)
    res = @backend.delete_class :domain => domain, :class => klass
    return !res.nil?
  end

  ##
  # Creates a new host named +host+.  +args+ must contain :ip and :port.
  # Returns true if successful, false if not.

  def create_host(host, args = {})
    raise ArgumentError, "Must specify ip and port" unless \
      args.include? :ip and args.include? :port

    return modify_host(host, args, 'create')
  end

  ##
  # Updates +host+ with +args+.  Returns true if successful, false if not.

  def update_host(host, args = {})
    return modify_host(host, args, 'update')
  end

  ##
  # Deletes host +host+.  Returns nil on failure.

  def delete_host(host)
    raise 'readonly mogilefs' if readonly?
    res = @backend.delete_host :host => host
    return !res.nil?
  end

  ##
  # Changes the device status of +device+ on +host+ to +state+ which can be
  # 'alive', 'down', or 'dead'.

  def change_device_state(host, device, state)
    raise 'readonly mogilefs' if readonly?
    res = @backend.set_state :host => host, :device => device, :state => state
    return !res.nil?
  end

  protected unless defined? $TESTING

  ##
  # Modifies +klass+ on +domain+ to store files on +mindevcount+ devices via
  # +action+.  Returns the class name if successful, nil if not.

  def modify_class(domain, klass, mindevcount, action)
    raise 'readonly mogilefs' if readonly?
    res = @backend.send("#{action}_class", :domain => domain, :class => klass,
                                          :mindevcount => mindevcount)

    return res['class'] unless res.nil?
  end

  ##
  # Modifies +host+ using +args+ via +action+.  Returns true if successful,
  # false if not.

  def modify_host(host, args = {}, action = 'create')
    args[:host] = host
    res = @backend.send "#{action}_host", args
    return !res.nil?
  end

  ##
  # Turns the response +res+ from the backend into an Array of Hashes from 1
  # to res[+count+].  If +underscore+ is true then a '_' character is assumed
  # between the prefix and the hash key value.
  #
  #   res = {"host1_remoteroot"=>"/mnt/mogilefs/rur-1",
  #          "host1_hostname"=>"rur-1",
  #          "host1_hostid"=>"1",
  #          "host1_http_get_port"=>"",
  #          "host1_altip"=>"",
  #          "hosts"=>"1",
  #          "host1_hostip"=>"",
  #          "host1_http_port"=>"",
  #          "host1_status"=>"alive",
  #          "host1_altmask"=>""}
  #   admin.clean 'hosts', 'host', res
  # 
  # Returns:
  # 
  #   [{"status"=>"alive",
  #     "http_get_port"=>"",
  #     "http_port"=>"",
  #     "hostid"=>"1",
  #     "hostip"=>"",
  #     "hostname"=>"rur-1",
  #     "remoteroot"=>"/mnt/mogilefs/rur-1",
  #     "altip"=>"",
  #     "altmask"=>""}]

  def clean(count, prefix, res, underscore = true)
    underscore = underscore ? '_' : ''
    return (1..res[count].to_i).map do |i|
      dev = res.select { |k,_| k =~ /^#{prefix}#{i}#{underscore}/ }.map do |k,v|
        [k.sub(/^#{prefix}#{i}#{underscore}/, ''), v]
      end
      Hash[*dev.flatten]
    end
  end

end

