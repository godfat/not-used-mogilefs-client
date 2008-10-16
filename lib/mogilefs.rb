##
# MogileFS is a Ruby client for Danga Interactive's open source distributed
# filesystem.
#
# To read more about Danga's MogileFS: http://danga.com/mogilefs/

module MogileFS

  VERSION = '1.3.1'

  ##
  # Raised when a socket remains unreadable for too long.

  class Error < StandardError; end
  class UnreadableSocketError < Error; end
  class ReadOnlyError < Error
    def message; 'readonly mogilefs'; end
  end
  class EmptyPathError < Error
    def message; 'Empty path for mogile upload'; end
  end

  class RequestTruncatedError < Error; end
  class InvalidResponseError < Error; end
  class UnreachableBackendError < Error
    def message; "couldn't connect to mogilefsd backend"; end
  end

end

require 'socket'

require 'mogilefs/backend'
require 'mogilefs/nfsfile'
require 'mogilefs/httpfile'
require 'mogilefs/client'
require 'mogilefs/mogilefs'
require 'mogilefs/admin'

