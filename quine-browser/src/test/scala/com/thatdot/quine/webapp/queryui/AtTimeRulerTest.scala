package com.thatdot.quine.webapp.queryui

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.webapp.queryui.AtTimeRuler._

/** The ruler's mapping between positions and offsets is what makes a drag trustworthy: the pin
  * must land where the offset says, pointing somewhere must name the offset shown there, and no
  * position may name the future.
  */
class AtTimeRulerTest extends AnyFunSuite with Matchers {

  private val second = 1000L
  private val minute = 60L * second
  private val hour = 60L * minute
  private val day = 24L * hour
  private val year = 365L * day

  test("now sits at the right end and the past runs leftward from it") {
    xOf(0L) shouldBe NowX
    xOf(-second) should be < NowX
    xOf(-minute) should be < xOf(-second)
  }

  test("an offset into the future has nowhere to go but now") {
    xOf(minute) shouldBe NowX
    xOf(30L * day) shouldBe NowX
  }

  test("ticks run leftward in order and clear the now dot") {
    val xs = ticks.map(_.x)
    xs shouldBe xs.sorted.reverse
    xs.distinct.size shouldBe xs.size
    xOf(-second) should be < (NowX - 10)
  }

  test("each decade further back is the same distance on the strip") {
    val decade = xOf(-10L * second) - xOf(-100L * second)
    (xOf(-second) - xOf(-10L * second)) shouldBe (decade +- 1e-6)
    (xOf(-100L * second) - xOf(-1000L * second)) shouldBe (decade +- 1e-6)
  }

  test("offsets beyond the far edge are clamped to it") {
    xOf(-100L * year) shouldBe LeftEdge
    xOf(-10L * year) shouldBe LeftEdge
    offStrip(-100L * year) shouldBe true
    offStrip(-year) shouldBe false
  }

  test("deltaOf inverts xOf across the strip") {
    Seq(-second, -37L * second, -minute, -3L * hour, -day, -40L * day, -year).foreach { delta =>
      math.round(deltaOf(xOf(delta))) shouldBe (delta +- math.max(1L, math.abs(delta) / 1000L))
    }
  }

  test("no position names the future, and the near end stands for a quarter second back") {
    deltaOf(NowX) shouldBe -MinMillis
    deltaOf(NowX + 20) shouldBe -MinMillis // past the end of the strip still means the near end
    deltaOf(Width) shouldBe -MinMillis
    deltaOf(NowX - 1e-9) should be < 0.0
    deltaOf(LeftEdge) shouldBe (-MaxMillis +- 1e-3)
    Seq(LeftEdge, LeftEdge + 100, NowX / 2, NowX - 1, NowX, NowX + 5, Width).foreach(x =>
      snappedDeltaOf(x) should be < 0L,
    )
  }

  test("pointing near a tick chooses exactly that tick's offset") {
    ticks.foreach { t =>
      snappedDeltaOf(t.x) shouldBe t.deltaMillis
      snappedDeltaOf(t.x + SnapDistance / 2) shouldBe t.deltaMillis
    }
  }

  test("pointing between ticks chooses the position's own offset") {
    val between = (xOf(-hour) + xOf(-6L * hour)) / 2
    snappedDeltaOf(between) shouldBe math.round(deltaOf(between))
    snappedDeltaOf(between) should be < -hour
    snappedDeltaOf(between) should be > -6L * hour
  }
}
