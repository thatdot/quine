package com.thatdot.quine.util

import scala.concurrent.duration.DurationInt

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.tagobjects.Retryable
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{Outcome, Retries}

class SizeAndTimeBoundedCacheTest extends AnyFlatSpec with Retries {
  override def withFixture(test: NoArgTest): Outcome =
    if (isRetryable(test))
      withRetryOnFailure(Span(5, Seconds))(super.withFixture(test))
    else
      super.withFixture(test)

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

  it should "evict elements based on time expiry" taggedAs Retryable in {
    val lru = new ExpiringLruSet.SizeAndTimeBounded[Int](10, 10, 150.milliseconds.toNanos) {
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

    Thread.sleep(100)

    lru.update(0)
    assert(lru.size == 2)
    assert(lru.iterator.toList == List(1, 0))

    lru.update(2)
    assert(lru.size == 3)
    assert(lru.iterator.toList == List(1, 0, 2))

    Thread.sleep(100)

    // 1 is evicted since it hasn't been accessed for 200ms
    lru.doExpiration()
    assert(lru.size == 2)
    assert(lru.iterator.toList == List(0, 2))

    Thread.sleep(200)

    // all are evicted
    lru.doExpiration()
    assert(lru.size == 0)
    assert(lru.iterator.toList == List())
  }

  it should "support declining an eviction" taggedAs Retryable in {
    val lru = new ExpiringLruSet.SizeAndTimeBounded[Int](3, 3, 150.milliseconds.toNanos) {
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

    Thread.sleep(200)

    // Decline evicting `0` on time constraint
    lru.doExpiration()
    assert(lru.size == 1)
    assert(lru.iterator.toList == List(0))
  }

}
