module MogileFS::Util

  CHUNK_SIZE = 65536

  # for copying large files while avoiding GC thrashing as much as possible
  def sysrwloop(io_rd, io_wr)
    copied = 0
    # avoid making sysread repeatedly allocate a new String
    # This is not well-documented, but both read/sysread can take
    # an optional second argument to use as the buffer to avoid
    # GC overhead of creating new strings in a loop
    buf = ' ' * CHUNK_SIZE # preallocate to avoid GC thrashing
    io_wr.sync = true
    loop do
      begin
        io_rd.sysread(CHUNK_SIZE, buf)
        loop do
          w = io_wr.syswrite(buf)
          copied += w
          break if w == buf.size
          buf = buf[w..-1]
        end
      rescue EOFError
        break
      end
    end
    copied
  end # sysrwloop

end
