require 'rubygems'
require 'hoe'

$:.unshift 'lib'
require 'mogilefs'

Hoe.new 'mogilefs-client', MogileFS::VERSION do |p|
  p.rubyforge_name = 'seattlerb'
  p.author = [ 'Eric Wong', 'Eric Hodel' ]
  p.email = 'normalperson@yhbt.net' # (Eric Wong)
  # p.email = 'drbrain@segment7.net' # (Eric Hodel)
  p.summary = p.paragraphs_of('README.txt', 1).first
  p.description = p.paragraphs_of('README.txt', 9).first
  p.url = p.paragraphs_of('README.txt', 5).first
  p.changes = p.paragraphs_of('History.txt', 0..1).join("\n\n")

  p.extra_dev_deps << ['ZenTest', '>= 3.6.1']
end

task :fix_perms do
  IO.popen('git ls-tree -r HEAD^0') do |fp|
    fp.each_line do |line|
      mode, type, sha1, path = line.chomp.split(/\s+/)
      case mode
      when '100644' then File.chmod(0644, path)
      when '100755' then File.chmod(0755, path)
      end
    end
  end
end

# vim: syntax=Ruby

