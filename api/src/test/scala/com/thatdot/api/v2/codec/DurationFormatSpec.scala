package com.thatdot.api.v2.codec

import scala.concurrent.duration._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DurationFormatSpec extends AnyFunSuite with Matchers {

  // ---- parse ----

  test("parses single-segment durations with each supported unit") {
    DurationFormat.parse("100ns") shouldBe Right(100.nanos)
    DurationFormat.parse("100us") shouldBe Right(100.micros)
    DurationFormat.parse("100µs") shouldBe Right(100.micros)
    DurationFormat.parse("500ms") shouldBe Right(500.millis)
    DurationFormat.parse("20s") shouldBe Right(20.seconds)
    DurationFormat.parse("3m") shouldBe Right(3.minutes)
    DurationFormat.parse("2h") shouldBe Right(2.hours)
  }

  test("parses fractional values") {
    DurationFormat.parse("1.5m") shouldBe Right(90.seconds)
    DurationFormat.parse("0.5s") shouldBe Right(500.millis)
  }

  test("parses compound durations by summing segments") {
    DurationFormat.parse("1h30m") shouldBe Right(90.minutes)
    DurationFormat.parse("2h45m30s") shouldBe Right((2 * 3600 + 45 * 60 + 30).seconds)
    DurationFormat.parse("1m500ms") shouldBe Right(60500.millis)
  }

  test("parses zero") {
    DurationFormat.parse("0s") shouldBe Right(Duration.Zero)
    DurationFormat.parse("0ms") shouldBe Right(Duration.Zero)
  }

  test("parses negative durations") {
    DurationFormat.parse("-30s") shouldBe Right(-30.seconds)
  }

  test("rejects empty and whitespace-only strings") {
    DurationFormat.parse("") shouldBe a[Left[_, _]]
    DurationFormat.parse("   ") shouldBe a[Left[_, _]]
  }

  test("rejects bare numbers (missing unit)") {
    DurationFormat.parse("20") shouldBe a[Left[_, _]]
  }

  test("rejects unknown units") {
    DurationFormat.parse("20d") shouldBe a[Left[_, _]] // days not supported
    DurationFormat.parse("20week") shouldBe a[Left[_, _]]
  }

  test("rejects garbage between segments") {
    DurationFormat.parse("20s and 30s") shouldBe a[Left[_, _]]
    DurationFormat.parse("abc") shouldBe a[Left[_, _]]
  }

  // ---- render ----

  test("render emits compound h/m/s for ≥1s, single-unit for sub-second") {
    DurationFormat.render(2.hours) shouldBe "2h"
    DurationFormat.render(90.minutes) shouldBe "1h30m"
    DurationFormat.render(20.seconds) shouldBe "20s"
    DurationFormat.render(500.millis) shouldBe "500ms"
    DurationFormat.render(100.micros) shouldBe "100us"
    DurationFormat.render(100.nanos) shouldBe "100ns"
  }

  test("render compound durations omit zero h/m/s components") {
    DurationFormat.render(1.hour + 30.seconds) shouldBe "1h30s"
    DurationFormat.render(5.hours + 30.minutes + 45.seconds) shouldBe "5h30m45s"
    DurationFormat.render(2.hours + 45.minutes) shouldBe "2h45m"
  }

  test("render zero is '0s'") {
    DurationFormat.render(Duration.Zero) shouldBe "0s"
  }

  test("render negative durations") {
    DurationFormat.render(-30.seconds) shouldBe "-30s"
  }

  test("render emits sub-second remainder alongside whole seconds when present") {
    DurationFormat.render(1.second + 500.millis) shouldBe "1s500ms"
    DurationFormat.render(500.millis) shouldBe "500ms" // sub-second only
  }

  // ---- round trip ----

  test("round trip preserves duration value through parse → render → parse") {
    val cases = Seq("0s", "20s", "500ms", "1h", "90m", "100us", "100ns", "-30s")
    for (s <- cases) {
      val first = DurationFormat.parse(s).toOption.get
      val rendered = DurationFormat.render(first)
      val second = DurationFormat.parse(rendered).toOption.get
      first shouldBe second
    }
  }
}
