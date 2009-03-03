require 'test/setup'
require 'mogilefs/bigfile'

class TestMogileFS__Bigfile < TestMogileFS
  include MogileFS::Bigfile

  def setup
    @klass = MogileFS::MogileFS
    super
  end

  def test_parser
    expect = {:type=>"file",
     :des=>"no description",
     :chunks=>2,
     :parts=>
      [nil,
       {:md5=>"d3b4d15c294b24d9f853e26095dfe3d0",
        :paths=>
         ["http://foo1:7500/dev2/0/000/144/0000144411.fid",
          "http://foo2:7500/dev1/0/000/144/0000144411.fid"],
        :bytes=>12},
       {:md5=>"d3b4d15c294b24d9f853e26095dfe3d0",
        :paths=>
         ["http://foo4:7500/dev2/0/000/144/0000144411.fid",
          "http://foo3:7500/dev1/0/000/144/0000144411.fid"],
        :bytes=>6}],
     :size=>18,
     :filename=>"foo.tar",
     :compressed=>false}

    s = <<EOS
des no description
type file
compressed 0
filename foo.tar
chunks 2
size 18

part 1 bytes=12 md5=d3b4d15c294b24d9f853e26095dfe3d0 paths: http://foo1:7500/dev2/0/000/144/0000144411.fid, http://foo2:7500/dev1/0/000/144/0000144411.fid
part 2 bytes=6 md5=d3b4d15c294b24d9f853e26095dfe3d0 paths: http://foo4:7500/dev2/0/000/144/0000144411.fid, http://foo3:7500/dev1/0/000/144/0000144411.fid
EOS
    i = parse_info(s)
    assert_equal expect, i
  end

end

