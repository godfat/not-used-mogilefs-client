require 'zlib'
require 'digest/md5'
require 'uri'
require 'mogilefs/util'

module MogileFS::Bigfile
  GZIP_HEADER = "\x1f\x8b".freeze # mogtool(1) has this
  # VALID_TYPES = %w(file tarball partition).map { |x| x.freeze }.freeze

  # returns a big_info hash if successful
  def bigfile_stat(key)
    parse_info(get_file_data(key))
  end

  # returns total bytes written and the big_info hash if successful, raises an
  # exception if not wr_io is expected to be an IO-like object capable of
  # receiving the syswrite method.
  def bigfile_write(key, wr_io, opts = { :verify => false })
    info = bigfile_stat(key)
    zi = nil
    md5 = opts[:verify] ? Digest::MD5.new : nil
    total = 0

    # we only decode raw zlib deflated streams that mogtool (unfortunately)
    # generates.  tarballs and gzip(1) are up to to the application to decrypt.
    filter = Proc.new do |buf|
      if zi == nil
        if info[:compressed] && info[:type] == 'file' &&
             buf.length >= 2 && buf[0,2] != GZIP_HEADER
          zi = Zlib::Inflate.new

          # mogtool(1) seems to have a bug that causes it to generate bogus
          # MD5s if zlib deflate is used.  Don't trust those MD5s for now...
          md5 = nil
        else
          zi = false
        end
      end
      buf ||= ''
      if zi
        zi.inflate(buf)
      else
        md5 << buf
        buf
      end
    end if (info[:compressed] || md5)

    info[:parts].each_with_index do |part,part_nr|
      next if part_nr == 0 # info[:parts][0] is always empty
      uris = verify_uris(part[:paths].map { |path| URI.parse(path) })
      if uris.empty?
        # part[:paths] may not be valid anymore due to rebalancing, however we
        # can get_keys on key,<part_nr> and retry paths if all paths fail
        part[:paths] = get_paths("#{key.gsub(/^big_info:/, '')},#{part_nr}")
        uris = verify_uris(part[:paths].map { |path| URI.parse(path) })
        raise MogileFS::Backend::NoDevices if uris.empty?
      end

      sock = http_read_sock(uris[0])
      md5.reset if md5
      w = sysrwloop(sock, wr_io, filter)

      if md5 && md5.hexdigest != part[:md5]
        raise MogileFS::ChecksumMismatchError, "#{md5} != #{part[:md5]}"
      end
      total += w
    end

    syswrite_full(wr_io, zi.finish) if zi

    [ total, info ]
  end

  private

    include MogileFS::Util

    ##
    # parses the contents of a _big_info: string or IO object
    def parse_info(info = '')
      rv = { :parts => [] }
      info.each_line do |line|
        line.chomp!
        case line
        when /^(des|type|filename)\s+(.+)$/
          rv[$1.to_sym] = $2
        when /^compressed\s+([01])$/
          rv[:compressed] = ($1 == '1')
        when /^(chunks|size)\s+(\d+)$/
          rv[$1.to_sym] = $2.to_i
        when /^part\s+(\d+)\s+bytes=(\d+)\s+md5=(.+)\s+paths:\s+(.+)$/
          rv[:parts][$1.to_i] = {
            :bytes => $2.to_i,
            :md5 => $3.downcase,
            :paths => $4.split(/\s*,\s*/),
          }
        end
      end

      rv
    end

end # module MogileFS::Bigfile

__END__
# Copied from mogtool:
# http://code.sixapart.com/svn/mogilefs/utils/mogtool, r1221

# this is a temporary file that we delete when we're doing recording all chunks

_big_pre:<key>

    starttime=UNIXTIMESTAMP

# when done, we write the _info file and delete the _pre.

_big_info:<key>

    des Cow's ljdb backup as of 2004-11-17
    type  { partition, file, tarball }
    compressed {0, 1}
    filename  ljbinlog.305.gz
    partblocks  234324324324


    part 1 <bytes> <md5hex>
    part 2 <bytes> <md5hex>
    part 3 <bytes> <md5hex>
    part 4 <bytes> <md5hex>
    part 5 <bytes> <md5hex>

_big:<key>,<n>
_big:<key>,<n>
_big:<key>,<n>


Receipt format:

BEGIN MOGTOOL RECIEPT
type partition
des Foo
compressed foo

part 1 bytes=23423432 md5=2349823948239423984 paths: http://dev5/2/23/23/.fid, http://dev6/23/423/4/324.fid
part 1 bytes=23423432 md5=2349823948239423984 paths: http://dev5/2/23/23/.fid, http://dev6/23/423/4/324.fid
part 1 bytes=23423432 md5=2349823948239423984 paths: http://dev5/2/23/23/.fid, http://dev6/23/423/4/324.fid
part 1 bytes=23423432 md5=2349823948239423984 paths: http://dev5/2/23/23/.fid, http://dev6/23/423/4/324.fid


END RECIEPT



