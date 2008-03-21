require 'mogilefs/backend'

##
# NFSFile wraps up the new file operations for storing files onto an NFS
# storage node.
#
# You really don't want to create an NFSFile by hand.  Instead you want to
# create a new file using MogileFS::MogileFS.new_file.

class MogileFS::NFSFile < File

  ##
  # The path of this file not including the local mount point.

  attr_reader :path

  ##
  # The key for this file.  This key won't represent a real file until you've
  # called #close.

  attr_reader :key

  ##
  # The class of this file.

  attr_reader :class

  class << self

    ##
    # Wraps up File.new with MogileFS-specific data.  Use
    # MogileFS::MogileFS#new_file instead of this method.

    def new(mg, fid, path, devid, klass, key)
      fp = super join(mg.root, path), 'w+'
      fp.send :setup, mg, fid, path, devid, klass, key
      return fp
    end

    ##
    # Wraps up File.open with MogileFS-specific data.  Use
    # MogileFS::MogileFS#new_file instead of this method.

    def open(mg, fid, path, devid, klass, key, &block)
      fp = new mg, fid, path, devid, klass, key

      return fp if block.nil?

      begin
        yield fp
      ensure
        fp.close
      end
    end

  end

  ##
  # Closes the file handle and marks it as closed in MogileFS.

  def close
    super
    @mg.backend.create_close(:fid => @fid, :devid => @devid,
                             :domain => @mg.domain, :key => @key,
                             :path => @path)
    return nil
  end

  private

  def setup(mg, fid, path, devid, klass, key)
    @mg = mg
    @fid = fid
    @path = path
    @devid = devid
    @klass = klass
    @key = key
  end

end

