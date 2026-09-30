package com.thatdot.quine.webapp.queryui

import scala.collection.mutable
import scala.scalajs.js

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.webapp.queryui.AtTimeInput._

/** The query-time dialog's resolution is a pure function of its text and the clock; these pin
  * down that the grammar produces the `Option[Long]` the explorer stores, that an offset is a
  * fixed instant rather than something that slides, and that what the ruler writes into the
  * field reads back as the instant it was given.
  *
  * Free-text cases stop short of Sugar's natural-language fallback, which the test bundle does
  * not load. A stand-in takes its place ([[handedToSugar]]) that records each text it is given
  * and reads none of them as a date, so a case can check that text left the grammar for the
  * phrase parser without exercising Sugar itself; the grammar handled here (`now`, epoch millis,
  * offsets) is the part with rules of its own.
  */
class AtTimeInputTest extends AnyFunSuite with Matchers {

  private val now = 1_700_000_000_000L
  private val instant = 1_622_282_520_004L // 2021-05-29T10:02:00.004Z

  /** Every text the grammar handed to Sugar, whose global this stand-in provides. */
  private val handedToSugar: mutable.Buffer[String] = mutable.Buffer.empty
  locally {
    val create: js.Function1[String, js.Date] = text => { handedToSugar += text; new js.Date(Double.NaN) }
    val isValid: js.Function1[js.Date, Boolean] = _ => false
    val sugar = js.Dynamic.literal(Date = js.Dynamic.literal(create = create, isValid = isValid))
    js.Dynamic.global.globalThis.updateDynamic("Sugar")(sugar)
  }

  private def term(amount: String, unit: OffsetUnit): Seq[OffsetTerm] = Seq(OffsetTerm(amount, unit))

  test("offset resolves to one fixed instant relative to the clock it was given") {
    resolveOffset(term("15", OffsetUnit.Seconds), OffsetDirection.Before, now) shouldBe Pinned(now - 15_000L)
    resolveOffset(term("2", OffsetUnit.Hours), OffsetDirection.After, now) shouldBe Pinned(now + 2L * 60L * 60L * 1000L)
    resolveOffset(term("1.5", OffsetUnit.Minutes), OffsetDirection.Before, now) shouldBe Pinned(now - 90_000L)
    resolveOffset(term("0", OffsetUnit.Days), OffsetDirection.Before, now) shouldBe Pinned(now)
  }

  test("offset rejects blanks, words, negative amounts, and no terms at all") {
    resolveOffset(term("", OffsetUnit.Seconds), OffsetDirection.Before, now) shouldBe an[Invalid]
    resolveOffset(term("ten", OffsetUnit.Seconds), OffsetDirection.Before, now) shouldBe an[Invalid]
    resolveOffset(term("-3", OffsetUnit.Seconds), OffsetDirection.Before, now) shouldBe an[Invalid]
    resolveOffset(Seq.empty, OffsetDirection.Before, now) shouldBe an[Invalid]
  }

  test("the same offset resolved against two clocks gives two different instants") {
    val first = resolveOffset(term("15", OffsetUnit.Seconds), OffsetDirection.Before, now)
    val later = resolveOffset(term("15", OffsetUnit.Seconds), OffsetDirection.Before, now + 60_000L)
    first should not be later
  }

  test("terms of an offset add up, in the short form and the long, with or without spaces") {
    val hour = 3_600_000L
    val day = 24L * hour
    resolveText("-1h30m", now) shouldBe Pinned(now - hour - 30L * 60_000L)
    resolveText("-1h 30m", now) shouldBe Pinned(now - hour - 30L * 60_000L)
    resolveText("- 1 hour 30 minutes", now) shouldBe Pinned(now - hour - 30L * 60_000L)
    resolveText("-1y1m1s", now) shouldBe Pinned(now - 365L * day - 60_000L - 1_000L)
    resolveText("-1y 1mo 1d", now) shouldBe Pinned(now - 365L * day - 30L * day - day)
    resolveText("-1d1d", now) shouldBe Pinned(now - 2L * day) // a repeated unit simply adds
    resolveText("+1h30m", now) shouldBe an[Invalid] // still the future
    resolveText("-1h 30x", now) shouldBe an[Invalid] // an unknown unit is not an offset at all
    handedToSugar should contain("-1h 30x") // it was left to the phrase parser, which had no use for it
  }

  test("free text keeps the old prompt's grammar") {
    resolveText("now", now) shouldBe Live
    resolveText("1622282520004", now) shouldBe Pinned(instant)
    resolveText(" 1622282520004 ", now) shouldBe Pinned(instant)
    resolveText("-15s", now) shouldBe Pinned(now - 15_000L)
    resolveText("- 2 hours", now) shouldBe Pinned(now - 7_200_000L)
    resolveText("-1.5min", now) shouldBe Pinned(now - 90_000L)
    resolveText("-3 days", now) shouldBe Pinned(now - 3L * 86_400_000L)
    resolveText("", now) shouldBe an[Invalid]
  }

  test("a moment after now is rejected however it is written, and the reason says so") {
    resolveText("+ 2 hours", now) shouldBe Invalid("In 2 hours is in the future, nothing has been recorded there.")
    resolveText("+1s", now) shouldBe an[Invalid]
    resolveText((now + 1L).toString, now) shouldBe an[Invalid]
    // A future date string takes the Sugar path, which the stand-in here reads nothing on; the
    // check sits after every path, so the epoch and shorthand cases above cover it.
  }

  test("now itself and every past moment still resolve") {
    resolveText(now.toString, now) shouldBe Pinned(now)
    resolveText("-0s", now) shouldBe Pinned(now)
    resolveText("+0s", now) shouldBe Pinned(now)
    resolveText((now - 1L).toString, now) shouldBe Pinned(now - 1L)
  }

  test("now is live whatever its case, rather than a pin at whatever Sugar makes of it") {
    Seq("Now", "NOW", " nOw ").foreach(text => resolveText(text, now) shouldBe Live)
  }

  test("a moment before the epoch is rejected however it is written, and the reason says so") {
    val reason = Invalid("That is before 1970, nothing has been recorded there.")
    resolveText("-1", now) shouldBe reason
    resolveText(Long.MinValue.toString, now) shouldBe reason
    resolveText("-999999999999d", now) shouldBe reason
    resolveText("0", now) shouldBe Pinned(EarliestMillis)
  }

  test("the clock is read exactly once per resolution, so the offset and the bounds agree") {
    var reads = 0
    def ticking: Long = { reads += 1; now + reads * 1000L }
    resolveText("-15s", ticking) shouldBe Pinned(now + 1000L - 15_000L)
    reads shouldBe 1
    resolveText("now", ticking) shouldBe Live
    resolveText("", ticking) shouldBe an[Invalid]
    resolveText(now.toString, ticking) shouldBe Pinned(now)
    reads shouldBe 4
  }

  test("the initial text reflects the moment currently tracked") {
    initialText(None) shouldBe "now"
    initialText(Some(instant)) shouldBe "2021-05-29T10:02:00.004Z"
    resolveText(initialText(None), now) shouldBe Live
  }

  test("shorthand snaps to a whole number of the most readable unit") {
    shorthand(now - 15_000L, now) shouldBe "-15s"
    shorthand(now - 15_400L, now) shouldBe "-15s"
    shorthand(now - 90_000L, now) shouldBe "-90s" // under two minutes stays in seconds
    shorthand(now - 3L * 3_600_000L, now) shouldBe "-3h"
    shorthand(now - 100L * 60_000L, now) shouldBe "-100m" // under two hours stays in minutes
    shorthand(now - 130L * 60_000L, now) shouldBe "-2h" // past two hours tips into hours, and rounds
    shorthand(now - 47L * 86_400_000L, now) shouldBe "-47d"
    shorthand(now + 2L * 86_400_000L, now) shouldBe "+2d"
  }

  test("shorthand names a whole offset in the largest unit that divides it, as the ruler's ticks read") {
    shorthand(now - 60_000L, now) shouldBe "-1m"
    shorthand(now - 3_600_000L, now) shouldBe "-1h"
    shorthand(now - 3_600_400L, now) shouldBe "-1h" // rounded to the minute first, then promoted
    shorthand(now - 6L * 3_600_000L, now) shouldBe "-6h"
    shorthand(now - 86_400_000L, now) shouldBe "-1d"
    shorthand(now - 7L * 86_400_000L, now) shouldBe "-1w"
    shorthand(now - 30L * 86_400_000L, now) shouldBe "-1mo"
    shorthand(now - 365L * 86_400_000L, now) shouldBe "-1y"
    shorthand(now - 3650L * 86_400_000L, now) shouldBe "-10y"
    shorthand(now - 14L * 86_400_000L, now) shouldBe "-2w"
    shorthand(now - 47L * 86_400_000L, now) shouldBe "-47d" // no larger unit divides it
    shorthand(now - 3L * 60_000L, now) shouldBe "-3m" // three minutes, not 180 seconds
  }

  test("weeks, months, and years are fixed spans in the grammar, as on the ruler") {
    resolveText("-1w", now) shouldBe Pinned(now - 7L * 86_400_000L)
    resolveText("- 2 weeks", now) shouldBe Pinned(now - 14L * 86_400_000L)
    resolveText("-1mo", now) shouldBe Pinned(now - 30L * 86_400_000L)
    resolveText("-3 months", now) shouldBe Pinned(now - 90L * 86_400_000L)
    resolveText("-1y", now) shouldBe Pinned(now - 365L * 86_400_000L)
    resolveText("-2yrs", now) shouldBe Pinned(now - 730L * 86_400_000L)
    resolveText("-1m", now) shouldBe Pinned(now - 60_000L) // m stays minutes
    resolveText("-1M", now) shouldBe Pinned(now - 60_000L)
  }

  test("shorthand never names an offset of zero, so the field is never emptied of meaning") {
    shorthand(now, now) shouldBe "-1s"
    shorthand(now - 100L, now) shouldBe "-1s"
    shorthand(now + 100L, now) shouldBe "+1s"
  }

  test("what the ruler writes reads back as the instant it was given, to the unit") {
    // Only past offsets: the ruler ends at now, and a future shorthand would be rejected anyway.
    Seq(
      -15_000L,
      -3L * 3_600_000L,
      -7L * 86_400_000L,
      -30L * 86_400_000L,
      -47L * 86_400_000L,
      -365L * 86_400_000L,
      -400L * 86_400_000L,
    ).foreach { delta =>
      resolveText(shorthand(now + delta, now), now) shouldBe Pinned(now + delta)
    }
  }

  test("every format example with rules of its own resolves, and none lies in the future") {
    // The phrases and the date strings, ISO included, go to Sugar, which the stand-in here
    // reads nothing on.
    val sugar = Set("six seconds ago", "today", "yesterday", "2021-05-29T10:02:00.004Z", "6:47 PM December 21, 2021")
    val examples = formats.flatMap(_.examples)
    examples.filterNot(sugar) should not be empty
    examples.filterNot(sugar).foreach { text =>
      resolveText(text, now) should not be an[Invalid]
    }
  }

  test("the ISO and epoch examples name the same instant, so the two rows read as one moment") {
    formats.flatMap(_.examples) should contain(formatIso(instant))
    resolveText(instant.toString, now) shouldBe Pinned(instant)
    formats.flatMap(_.examples) should contain(instant.toString)
  }

  test("describeRelative rounds to the nearest sensible unit") {
    describeRelative(now, now) shouldBe "just now"
    describeRelative(now - 15_000L, now) shouldBe "15 seconds ago"
    describeRelative(now - 1_000L, now) shouldBe "just now"
    describeRelative(now - 60_000L, now) shouldBe "1 minute ago"
    describeRelative(now + 3L * 3_600_000L, now) shouldBe "in 3 hours"
    describeRelative(now - 2L * 86_400_000L, now) shouldBe "2 days ago"
    describeRelative(now - 400L * 86_400_000L, now) shouldBe "1 year ago"
  }

  test("formatIso is the UTC instant") {
    formatIso(instant) shouldBe "2021-05-29T10:02:00.004Z"
  }
}
