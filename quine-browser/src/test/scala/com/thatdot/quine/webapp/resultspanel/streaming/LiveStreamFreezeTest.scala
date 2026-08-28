package com.thatdot.quine.webapp.resultspanel.streaming

import com.raquo.airstream.eventbus.EventBus
import com.raquo.airstream.ownership.ManualOwner
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Frames buffer into `pending` and only become visible rows on a 100ms throttle tick, so
  * anything that stops the stream has to drain first or the last window's rows vanish.
  *
  * This is a regression guard for a real failure: a background query sends its whole result
  * set and its completion frame in one burst, the completion drove `freeze()`, and the tick had
  * not run even once — so every row was discarded and the results card rendered empty while the
  * WebSocket had plainly delivered the data.
  */
class LiveStreamFreezeTest extends AnyFunSuite with Matchers {

  private def row(i: Int): Json = Json.obj("i" -> Json.fromInt(i))

  /** Feed frames and freeze without ever letting the throttle tick fire — the burst-then-end
    * shape of a fast background query.
    */
  private def burstThenFreeze(count: Int): LiveStream = {
    val bus = new EventBus[Json]
    val stream = new LiveStream
    // A subscriber must exist for the bus to deliver; `connect` provides it.
    stream.connect(bus.events)
    (1 to count).foreach(i => bus.writer.onNext(row(i)))
    stream.freeze()
    stream
  }

  test("rows received since the last tick survive a freeze") {
    val stream = burstThenFreeze(3)
    stream.rows.now().map(_.data) shouldBe Vector(row(1), row(2), row(3))
  }

  test("their columns survive too, so the table has something to render") {
    burstThenFreeze(3).columns.now() shouldBe Vector("i")
  }

  test("freezing an empty stream stays empty rather than inventing a row") {
    val stream = new LiveStream
    stream.connect(new EventBus[Json].events)
    stream.freeze()
    stream.rows.now() shouldBe empty
  }

  test("freeze is idempotent — a card can be frozen from several paths") {
    val stream = burstThenFreeze(2)
    stream.freeze()
    stream.freeze()
    stream.rows.now() should have size 2
  }

  test("a frozen stream ignores frames that arrive afterwards") {
    val bus = new EventBus[Json]
    val stream = new LiveStream
    stream.connect(bus.events)
    bus.writer.onNext(row(1))
    stream.freeze()
    bus.writer.onNext(row(2))
    stream.freeze()
    stream.rows.now().map(_.data) shouldBe Vector(row(1))
  }

  test("the drained batch still respects the row budget") {
    val bus = new EventBus[Json]
    val stream = new LiveStream
    var filled = 0
    stream.connect(bus.events, budget = () => Some(2), onBudgetFilled = () => filled += 1)
    (1 to 5).foreach(i => bus.writer.onNext(row(i)))
    stream.freeze()
    // Capped at the budget, and the owner told exactly once that the session is over.
    stream.rows.now().map(_.data) shouldBe Vector(row(1), row(2))
    filled shouldBe 1
  }

  test("a manual owner can still observe the flushed rows") {
    // Guards the ordering inside freeze: the drain must happen before the stream owner's
    // subscriptions are killed, or the flush would land after teardown.
    val bus = new EventBus[Json]
    val stream = new LiveStream
    stream.connect(bus.events)
    var seen = Vector.empty[Int]
    stream.rows.signal.foreach(rs => seen = rs.map(_.seq.toInt))(new ManualOwner)
    bus.writer.onNext(row(1))
    stream.freeze()
    seen shouldBe Vector(1)
  }
}
