require 'test/unit'

$TESTING = true

require 'mogilefs/pool'

class MogileFS::Pool

  attr_reader :objects, :queue

end

class Resource; end

class ResourceWithArgs

  def initialize(args)
  end

end

class TestPool < Test::Unit::TestCase

  def setup
    @pool = MogileFS::Pool.new Resource
  end

  def test_get
    o1 = @pool.get
    o2 = @pool.get
    assert_kind_of Resource, o1
    assert_kind_of Resource, o2
    assert_not_equal o1, o2
  end

  def test_get_with_args
    @pool = MogileFS::Pool.new ResourceWithArgs, 'my arg'
    o = @pool.get
    assert_kind_of ResourceWithArgs, o
  end

  def test_put
    o = @pool.get
    @pool.put o

    assert_raises(MogileFS::Pool::BadObjectError) { @pool.put nil }
    assert_raises(MogileFS::Pool::BadObjectError) { @pool.put Resource.new }
  end

  def test_put_destroy
    objs = (0...7).map { @pool.get } # pool full

    assert_equal 7, @pool.objects.length
    assert_equal 0, @pool.queue.length

    4.times { @pool.put objs.shift }

    assert_equal 7, @pool.objects.length
    assert_equal 4, @pool.queue.length

    @pool.put objs.shift # trip threshold

    assert_equal 4, @pool.objects.length
    assert_equal 2, @pool.queue.length

    @pool.put objs.shift # don't need to remove any more

    assert_equal 4, @pool.objects.length
    assert_equal 3, @pool.queue.length

    @pool.put objs.shift until objs.empty?

    assert_equal 4, @pool.objects.length
    assert_equal 4, @pool.queue.length
  end

  def test_use
    val = @pool.use { |o| assert_kind_of Resource, o }
    assert_equal nil, val, "Don't return object from pool"
  end

  def test_use_with_exception
    @pool.use { |o| raise } rescue nil
    assert_equal 1, @pool.queue.length, "Resource not returned to pool"
  end

  def test_use_reuse
    o1 = nil
    o2 = nil

    @pool.use { |o| o1 = o }
    @pool.use { |o| o2 = o }

    assert_equal o1, o2, "Objects must be reused"
  end

end

