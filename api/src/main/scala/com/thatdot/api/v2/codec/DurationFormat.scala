package com.thatdot.api.v2.codec

import scala.concurrent.duration.{FiniteDuration, NANOSECONDS}

/** Go-style duration string parsing and rendering, per AIP-142.
  *
  * A duration is one or more `<number><unit>` segments concatenated, e.g.:
  *
  *   - `"20s"` — 20 seconds
  *   - `"500ms"` — 500 milliseconds
  *   - `"1.5m"` — 1.5 minutes (90 seconds)
  *   - `"2h45m"` — 2 hours 45 minutes
  *   - `"5h30m45s"` — compound form
  *   - `"0s"` — zero
  *
  * Recognized units: `ns`, `us` (or `µs`), `ms`, `s`, `m`, `h`. Decimal numbers are
  * permitted within each segment. Negative durations (`"-30s"`) are accepted. The
  * empty string and any text outside the segment grammar are rejected.
  *
  * Rendering for durations of one second or longer emits compound `h`/`m`/`s` segments
  * (skipping zero components), so `5h30m45s` renders as `"5h30m45s"` rather than the
  * less-readable single-unit `"19845s"`. Sub-second durations render as a single
  * `ms`/`us`/`ns` segment.
  */
object DurationFormat {

  private val UnitNanos: Map[String, Long] = Map(
    "ns" -> 1L,
    "us" -> 1000L,
    "µs" -> 1000L,
    "ms" -> 1_000_000L,
    "s" -> 1_000_000_000L,
    "m" -> 60L * 1_000_000_000L,
    "h" -> 3600L * 1_000_000_000L,
  )

  private val Token = """(-?\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)""".r

  def parse(s: String): Either[String, FiniteDuration] = {
    val trimmed = s.trim
    if (trimmed.isEmpty) Left("Duration string is empty")
    else {
      val matches = Token.findAllMatchIn(trimmed).toList
      val covered = matches.map(_.matched).mkString
      if (matches.isEmpty || covered != trimmed)
        Left(s"Invalid duration '$s'; expected e.g. '20s', '500ms', '1.5m', '2h45m'")
      else {
        val totalNanos = matches.iterator.map { m =>
          val number = m.group(1).toDouble
          val nanosPerUnit = UnitNanos(m.group(2))
          (number * nanosPerUnit).toLong
        }.sum
        Right(FiniteDuration(totalNanos, NANOSECONDS))
      }
    }
  }

  /** Render a duration as an AIP-142 string. Durations of one second or more are
    * compound — `5h30m45s` rather than the less-readable `19845s` — while sub-second
    * durations use the largest single sub-second unit that yields an integer count.
    */
  def render(d: FiniteDuration): String = {
    val nanos = d.toNanos
    if (nanos == 0) "0s"
    else if (nanos < 0) "-" + renderPositive(-nanos)
    else renderPositive(nanos)
  }

  private def renderPositive(nanos: Long): String =
    if (nanos < UnitNanos("s")) {
      // Sub-second: largest single unit that yields an integer count
      if (nanos % UnitNanos("ms") == 0) s"${nanos / UnitNanos("ms")}ms"
      else if (nanos % UnitNanos("us") == 0) s"${nanos / UnitNanos("us")}us"
      else s"${nanos}ns"
    } else {
      // ≥ 1s: compound h/m/s, omitting zero components
      val sb = new StringBuilder
      val hours = nanos / UnitNanos("h")
      val afterHours = nanos % UnitNanos("h")
      val minutes = afterHours / UnitNanos("m")
      val afterMinutes = afterHours % UnitNanos("m")
      val seconds = afterMinutes / UnitNanos("s")
      val subSecondNanos = afterMinutes % UnitNanos("s")
      if (hours > 0) sb.append(hours).append('h')
      if (minutes > 0) sb.append(minutes).append('m')
      // Always emit a seconds component when there's sub-second precision; otherwise omit it
      // when zero (so a clean "5h30m" stays clean).
      if (seconds > 0 || subSecondNanos > 0 || (hours == 0 && minutes == 0)) {
        if (subSecondNanos == 0) sb.append(seconds).append('s')
        else if (seconds > 0) {
          sb.append(seconds).append('s')
          sb.append(renderSubSecond(subSecondNanos))
        } else {
          sb.append(renderSubSecond(subSecondNanos))
        }
      }
      sb.toString
    }

  private def renderSubSecond(nanos: Long): String =
    if (nanos % UnitNanos("ms") == 0) s"${nanos / UnitNanos("ms")}ms"
    else if (nanos % UnitNanos("us") == 0) s"${nanos / UnitNanos("us")}us"
    else s"${nanos}ns"
}
