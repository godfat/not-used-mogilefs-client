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

# vim: syntax=Ruby

