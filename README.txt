= mogilefs-client

A Ruby MogileFS client

Rubyforge Project:

http://rubyforge.org/projects/seattlerb/

Documentation:

http://seattlerb.org/mogilefs-client

File bugs:

http://rubyforge.org/tracker/?func=add&group_id=1513&atid=5921

== About

A Ruby MogileFS client.  MogileFS is a distributed filesystem written
by Danga Interactive.  This client supports NFS and HTTP modes.

For information on MogileFS see:

http://danga.com/mogilefs/

== Installing mogilefs-client

First you need a MogileFS setup.  You can find information on how to do that at the above URL.

Then install the gem:

  $ sudo gem install mogilefs-client

== Using mogilefs-client

  # Create a new instance that will communicate with these trackers:
  hosts = %w[192.168.1.69:6001 192.168.1.70:6001]
  mg = MogileFS::MogileFS.new(:domain => 'test', :hosts => hosts
                              :root => '/mnt/mogilefs')
  
  # Stores "A bunch of text to store" into 'some_key' with a class of 'text'.
  mg.store_content 'some_key', 'text', "A bunch of text to store"
  
  # Retrieve data from 'some_key'
  data = mg.get_file_data 'some_key'
  
  # Store the contents of 'image.jpeg' into the key 'my_image' with a class of
  # 'image'.
  mg.store_file 'my_image', 'image', 'image.jpeg'
  
  # Store the contents of 'image.jpeg' into the key 'my_image' with a class of
  # 'image' using an open IO.
  File.open 'image.jpeg' do |fp|
    mg.store_file 'my_image', 'image', fp
  end
  
  # Remove the key 'my_image' and 'some_key'.
  mg.delete 'my_image'
  mg.delete 'some_key'

== WARNING!

This client is only known to work in NFS mode.  HTTP mode is implemented but
only lightly tested in production environments.  If you find a bug,
please report it on the Rubyforge project.

