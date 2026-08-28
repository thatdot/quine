package com.thatdot.quine.webapp.dataservice

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Guards the sharing rule for background-query taps.
  *
  * The Explorer's result cards and the Streams page's viewer can watch the same run at the same
  * time, and both surfaces stay mounted. Releasing the socket when the first of them closes
  * would cut the other's stream — with no error, and no way for it to notice.
  */
class SubscriberRegistryTest extends AnyFunSuite with Matchers {

  test("the first subscriber opens the resource, later ones join it") {
    val registry = new SubscriberRegistry
    registry.add("explorer", "run-1") shouldBe true
    registry.add("streamsPage", "run-1") shouldBe false
  }

  test("closing one of two subscribers does not release the resource") {
    val registry = new SubscriberRegistry
    registry.add("explorer", "run-1")
    registry.add("streamsPage", "run-1")

    registry.remove("streamsPage", "run-1") shouldBe false
    registry.watching("run-1") shouldBe Set("explorer")
  }

  test("the last subscriber to leave releases it") {
    val registry = new SubscriberRegistry
    registry.add("explorer", "run-1")
    registry.add("streamsPage", "run-1")
    registry.remove("streamsPage", "run-1")

    registry.remove("explorer", "run-1") shouldBe true
    registry.watching("run-1") shouldBe empty
  }

  test("a repeated subscriber doesn't double-count, so one close still releases") {
    val registry = new SubscriberRegistry
    registry.add("explorer", "run-1")
    registry.add("explorer", "run-1")

    registry.remove("explorer", "run-1") shouldBe true
  }

  test("a duplicate close can't release a resource someone else is still watching") {
    val registry = new SubscriberRegistry
    registry.add("explorer", "run-1")
    registry.add("streamsPage", "run-1")
    registry.remove("streamsPage", "run-1") shouldBe false

    // The Streams page closing twice — a stale binder, a re-render — must not evict the
    // Explorer's card.
    registry.remove("streamsPage", "run-1") shouldBe false
    registry.watching("run-1") shouldBe Set("explorer")
  }

  test("closing an unknown subscriber or key is a no-op") {
    val registry = new SubscriberRegistry
    registry.remove("nobody", "run-1") shouldBe false
    registry.add("explorer", "run-1")
    registry.remove("nobody", "run-1") shouldBe false
    registry.watching("run-1") shouldBe Set("explorer")
  }

  test("resources are tracked independently") {
    val registry = new SubscriberRegistry
    registry.add("explorer", "run-1")
    registry.add("explorer", "run-2") shouldBe true

    registry.remove("explorer", "run-1") shouldBe true
    registry.watching("run-2") shouldBe Set("explorer")
  }

  test("clear forgets every watcher") {
    val registry = new SubscriberRegistry
    registry.add("explorer", "run-1")
    registry.add("streamsPage", "run-2")

    registry.clear()
    registry.watching("run-1") shouldBe empty
    registry.watching("run-2") shouldBe empty
    // After a teardown the next open is a first open again.
    registry.add("explorer", "run-1") shouldBe true
  }
}
