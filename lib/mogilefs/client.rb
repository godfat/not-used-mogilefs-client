require 'mogilefs/backend'

##
# MogileFS::Client is the MogileFS client base class.  Concrete clients like
# MogileFS::MogileFS and MogileFS::Admin are implemented atop this one to do
# real work.

class MogileFS::Client

  ##
  # The backend connection for this client

  attr_reader :backend

  attr_accessor :hosts if defined? $TESTING

  ##
  # Creates a new Client.  See MogileFS::Backend#initialize for how to specify
  # hosts.  If :readonly is set to true, the client will not modify anything
  # on the server.
  #
  #   MogileFS::Client.new :hosts => ['kaa:6001', 'ziz:6001'], :readonly => true

  def initialize(args)
    @hosts = args[:hosts]
    @readonly = args[:readonly] ? true : false
    @timeout = args[:timeout]

    reload
  end

  ##
  # Creates a new MogileFS::Backend.

  def reload
    @backend = MogileFS::Backend.new :hosts => @hosts, :timeout => @timeout
  end

  ##
  # The last error reported by the backend.
  #--
  # TODO use Exceptions

  def err
    @backend.lasterr
  end

  ##
  # The last error message reported by the backend.
  #--
  # TODO use Exceptions

  def errstr
    @backend.lasterrstr
  end

  ##
  # Is this a read-only client?

  def readonly?
    return @readonly
  end

end

