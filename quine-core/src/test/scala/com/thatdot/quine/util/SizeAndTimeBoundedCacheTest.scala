package com.thatdot.quine.util

import scala.concurrent.duration.DurationInt

import org.scalatest.flatspec.AnyFlatSpec

class TestTimeProvider(initialTime: Long) extends NanoTimeSource {
  private var currentTime: Long = initialTime
  def advanceByMillis(millis: Long): Unit = currentTime += millis * 1000L * 1000L

  def nanoTime(): Long = currentTime
}
class SizeAndTimeBoundedCacheTest extends AnyFlatSpec {

  "A SizeAndTimeBounded" should "evict elements when it exceeds its maximum capacity" in {
    val lru = new ExpiringLruSet.SizeAndTimeBounded[Int](3, 3, Long.MaxValue) {
      def shouldExpire(elem: Int) = ExpiringLruSet.ExpiryDecision.ShouldRemove
      def expiryListener(cause: ExpiringLruSet.RemovalCause, elem: Int) = ()
    }

    assert(lru.size == 0)
    assert(lru.iterator.toList == List())

    lru.update(0)
    assert(lru.size == 1)
    assert(lru.iterator.toList == List(0))

    lru.update(1)
    assert(lru.size == 2)
    assert(lru.iterator.toList == List(0, 1))

    lru.update(2)
    assert(lru.size == 3)
    assert(lru.iterator.toList == List(0, 1, 2))

    // 0 is evicted as the least recently accessed
    lru.update(3)
    assert(lru.size == 3)
    assert(lru.iterator.toList == List(1, 2, 3))
  }

  it should "evict elements based on the access order" in {
    val lru = new ExpiringLruSet.SizeAndTimeBounded[Int](2, 2, Long.MaxValue) {
      def shouldExpire(elem: Int) = ExpiringLruSet.ExpiryDecision.ShouldRemove
      def expiryListener(cause: ExpiringLruSet.RemovalCause, elem: Int) = ()
    }
    assert(lru.size == 0)
    assert(lru.iterator.toList == List())

    lru.update(0)
    assert(lru.size == 1)
    assert(lru.iterator.toList == List(0))

    lru.update(1)
    assert(lru.size == 2)
    assert(lru.iterator.toList == List(0, 1))

    lru.update(0)
    assert(lru.size == 2)
    assert(lru.iterator.toList == List(1, 0))

    // 1 is evicted as the least recently accessed
    lru.update(2)
    assert(lru.size == 2)
    assert(lru.iterator.toList == List(0, 2))
  }

  it should "evict elements based on time expiry" in {
    val time = new TestTimeProvider(0)
    val lru = new ExpiringLruSet.SizeAndTimeBounded[Int](10, 10, 150.milliseconds.toNanos, time) {
      def shouldExpire(elem: Int) = ExpiringLruSet.ExpiryDecision.ShouldRemove
      def expiryListener(cause: ExpiringLruSet.RemovalCause, elem: Int) = ()
    }
    assert(lru.size == 0)
    assert(lru.iterator.toList == List())

    lru.update(0)
    assert(lru.size == 1)
    assert(lru.iterator.toList == List(0))

    lru.update(1)
    assert(lru.size == 2)
    assert(lru.iterator.toList == List(0, 1))

    time.advanceByMillis(100)

    lru.update(0)
    assert(lru.size == 2)
    assert(lru.iterator.toList == List(1, 0))

    lru.update(2)
    assert(lru.size == 3)
    assert(lru.iterator.toList == List(1, 0, 2))

    time.advanceByMillis(100)

    // 1 is evicted since it hasn't been accessed for 200ms
    lru.doExpiration()
    assert(lru.size == 2)
    assert(lru.iterator.toList == List(0, 2))

    time.advanceByMillis(200)

    // all are evicted
    lru.doExpiration()
    assert(lru.size == 0)
    assert(lru.iterator.toList == List())
  }

  it should "support declining an eviction" in {
    val time = new TestTimeProvider(0)
    val lru = new ExpiringLruSet.SizeAndTimeBounded[Int](3, 3, 150.milliseconds.toNanos, time) {
      def shouldExpire(elem: Int): ExpiringLruSet.ExpiryDecision =
        if (elem != 0) ExpiringLruSet.ExpiryDecision.ShouldRemove // never evict 0!
        else ExpiringLruSet.ExpiryDecision.RejectRemoval(false)
      def expiryListener(cause: ExpiringLruSet.RemovalCause, elem: Int) = ()
    }
    assert(lru.size == 0)
    assert(lru.iterator.toList == List())

    lru.update(0)
    assert(lru.size == 1)
    assert(lru.iterator.toList == List(0))

    lru.update(1)
    assert(lru.size == 2)
    assert(lru.iterator.toList == List(0, 1))

    lru.update(2)
    assert(lru.size == 3)
    assert(lru.iterator.toList == List(0, 1, 2))

    // Decline evicting `0` on size constraint
    lru.update(3)
    assert(lru.size == 3)
    assert(lru.iterator.toList == List(2, 3, 0))

    time.advanceByMillis(200)

    // Decline evicting `0` on time constraint
    lru.doExpiration()
    assert(lru.size == 1)
    assert(lru.iterator.toList == List(0))
  }

}
