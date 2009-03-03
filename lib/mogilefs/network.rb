require 'mogilefs'
require 'socket'
require 'mogilefs/util'

module MogileFS::Network
  # given an array of URIs, verify that at least one of them is accessible
  # with the expected HTTP code within the timeout period (in seconds).
  def verify_uris(uris = [], expect = '200', timeout = 2.00)
    uri_socks = {}

    # first, we asynchronously connect to all of them
    uris.each do |uri|
      sock = Socket.mogilefs_new_nonblock(uri.host, uri.port) rescue next
      uri_socks[sock] = uri
    end

    # wait for at least one of them to finish connecting and send
    # HTTP requests to the connected ones
    sockets, timeout = get_writable_set(uri_socks, timeout)

    # Await a response from the sockets we had written to, we only need one
    # valid response, but we'll take more if they return simultaneously
    sockets[0] ? get_readable_uris(sockets, uri_socks, expect, timeout) : []

    ensure
      uri_socks.keys.each { |sock| sock.close rescue nil }
  end

  private
    include MogileFS::Util

    # returns an array of writeable Sockets and leftover from timeout
    def get_writable_set(uri_socks, timeout)
      sockets = []
      begin
        t0 = Time.now
        r = begin
         IO.select(nil, uri_socks.keys, nil, timeout > 0 ? timeout : 0)
        rescue
          # get rid of bad descriptors
          uri_socks.delete_if do |sock, uri|
            begin
              sock.recv_nonblock(1)
              false # should never get here for HTTP, really...
            rescue Errno::EAGAIN, Errno::EINTR
              false
            rescue
              sock.close rescue nil
              true
            end
          end
          timeout -= (Time.now - t0)
          retry if timeout >= 0
        end

        break unless r && r[1]

        r[1].each do |sock|
          begin
            # we don't about short/interrupted writes here, if the following
            # request fails or blocks then the server is flat-out hopeless
            sock.syswrite "HEAD #{uri_socks[sock].request_uri} HTTP/1.0\r\n\r\n"
            sockets << sock
          rescue
            sock.close rescue nil
          end
        end

        timeout -= (Time.now - t0)
      end until (sockets[0] || timeout < 0)

      [ sockets, timeout ]
    end

    # returns an array of URIs from uri_socks that are good
    def get_readable_uris(sockets, uri_socks, expect, timeout)
      ok_uris = []

      begin
        t0 = Time.now
        r = IO.select(sockets, nil, nil, timeout > 0 ? timeout : 0) rescue nil

        (r && r[0] ? r[0] : sockets).each do |sock|
          buf = begin
            sock.recv_nonblock(128, Socket::MSG_PEEK)
          rescue Errno::EAGAIN, Errno::EINTR
            next
          rescue
            sockets.delete(sock) # socket went bad
            next
          end

          if buf && /\AHTTP\/[\d\.]+ #{expect} / =~ buf
            ok_uris << uri_socks.delete(sock)
            sock.close rescue nil
          end
        end
        timeout -= (Time.now - t0)
      end until ok_uris[0] || timeout < 0

      ok_uris
    end

end # module MogileFS::Network
