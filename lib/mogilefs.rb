##
# MogileFS is a Ruby client for Danga Interactive's open source distributed
# filesystem.
#
# To read more about Danga's MogileFS: http://danga.com/mogilefs/

module MogileFS

  VERSION = '1.3.1'

  ##
  # Raised when a socket remains unreadable for too long.

  class UnreadableSocketError < RuntimeError; end

end

require 'socket'

require 'mogilefs/backend'
require 'mogilefs/nfsfile'
require 'mogilefs/httpfile'
require 'mogilefs/client'
require 'mogilefs/mogilefs'
require 'mogilefs/admin'

