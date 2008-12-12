require 'test/setup'

class TestMogileFS__Admin < TestMogileFS

  def setup
    @klass = MogileFS::Admin
    super
  end

  def test_clean
    res = {"host1_remoteroot"=>"/mnt/mogilefs/rur-1",
           "host1_hostname"=>"rur-1",
           "host1_hostid"=>"1",
           "host1_http_get_port"=>"",
           "host1_altip"=>"",
           "hosts"=>"1",
           "host1_hostip"=>"",
           "host1_http_port"=>"",
           "host1_status"=>"alive",
           "host1_altmask"=>""}
    actual = @client.clean 'hosts', 'host', res

    expected = [{"status"=>"alive",
                 "http_get_port"=>"",
                 "http_port"=>"",
                 "hostid"=>"1",
                 "hostip"=>"",
                 "hostname"=>"rur-1",
                 "remoteroot"=>"/mnt/mogilefs/rur-1",
                 "altip"=>"",
                 "altmask"=>""}]

    assert_equal expected, actual
  end

  def test_each_fid
    @backend.stats = {
      'fidmax' => '182',
      'fidcount' => '2',
    }

    @backend.list_fids = {
      'fid_count' => '1',
      'fid_1_fid' => '99',
      'fid_1_class' => 'normal',
      'fid_1_devcount' => '2',
      'fid_1_domain' => 'test',
      'fid_1_key' => 'file_key',
      'fid_1_length' => '4',
    }

    @backend.list_fids = {
      'fid_count' => '1',
      'fid_1_fid' => '182',
      'fid_1_class' => 'normal',
      'fid_1_devcount' => '2',
      'fid_1_domain' => 'test',
      'fid_1_key' => 'new_new_key',
      'fid_1_length' => '9',
    }

    fids = []
    @client.each_fid { |fid| fids << fid }

    expected = [
      { "fid"      => "99",
        "class"    => "normal",
        "domain"   => "test",
        "devcount" => "2",
        "length"   => "4",
        "key"      => "file_key" },
      { "fid"      => "182",
        "class"    => "normal",
        "devcount" => "2",
        "domain"   => "test",
        "length"   => "9",
        "key"      => "new_new_key" },
    ]

    assert_equal expected, fids
  end

  def test_get_domains
    @backend.get_domains = {
      'domains' => 2,
      'domain1' => 'test',
      'domain2' => 'images',
      'domain1classes' => '1',
      'domain2classes' => '2',
      'domain1class1name' => 'default',
      'domain1class1mindevcount' => '2',
      'domain2class1name' => 'default',
      'domain2class1mindevcount' => '2',
      'domain2class2name' => 'resize',
      'domain2class2mindevcount' => '1',
    }

    expected = {
      'test'   => { 'default' => 2, },
      'images' => { 'default' => 2, 'resize' => 1 },
    }

    assert_equal expected, @client.get_domains
  end

  def disabled_test_get_stats
    @backend.stats = {}

    expected = {
      'fids' => { 'max' => '99', 'count' => '2' },
      'device' => [
        { 'status' => 'alive', 'files' => '2', 'id' => '1', 'host' => 'rur-1' },
        { 'status' => 'alive', 'files' => '2', 'id' => '2', 'host' => 'rur-2' }
      ],
      'replication' => [
        { 'files' => '2', 'class' => 'normal', 'devcount' => '2',
          'domain' => 'test' }
      ],
      'file' => [{ 'files' => '2', 'class' => 'normal', 'domain' => 'test' }]
    }

    assert_equal
  end

  def test_get_stats_fids
    @backend.stats = {
      'fidmax' => 99,
      'fidcount' => 2,
    }

    expected = {
      'fids' => { 'max' => 99, 'count' => 2 },
    }

    assert_equal expected, @client.get_stats('all')
  end

  def test_list_fids
    @backend.list_fids = {
      'fid_count' => '2',
      'fid_1_fid' => '99',
      'fid_1_class' => 'normal',
      'fid_1_devcount' => '2',
      'fid_1_domain' => 'test',
      'fid_1_key' => 'file_key',
      'fid_1_length' => '4',
      'fid_2_fid' => '82',
      'fid_2_class' => 'normal',
      'fid_2_devcount' => '2',
      'fid_2_domain' => 'test',
      'fid_2_key' => 'new_new_key',
      'fid_2_length' => '9',
    }

    expected = [
      { "fid"      => "99",
        "class"    => "normal",
        "domain"   => "test",
        "devcount" => "2",
        "length"   => "4",
        "key"      => "file_key" },
      { "fid"      => "82",
        "class"    => "normal",
        "devcount" => "2",
        "domain"   => "test",
        "length"   => "9",
        "key"      => "new_new_key" },
    ]

    assert_equal expected, @client.list_fids(0, 100)
  end

end

