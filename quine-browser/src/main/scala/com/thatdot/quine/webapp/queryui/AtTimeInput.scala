package com.thatdot.quine.webapp.queryui

import scala.scalajs.js
import scala.util.Try
import scala.util.matching.Regex

import com.thatdot.quine.webapp.Sugar

/** Parsing, resolution, and formatting for the query-time dialog ([[AtTimeModal]]), kept free of
  * Laminar so it can be unit tested.
  *
  * The explorer's query time is an `Option[Long]`: `None` queries the present, `Some(millis)` pins
  * every query to one fixed instant. The dialog names that value with one text field, in the
  * grammar the old prompt accepted, and a ruler ([[AtTimeRuler]]) that writes into the same field.
  * An offset such as "15 seconds ago" is resolved against the clock once, when the user applies
  * it, and the resulting instant does not move afterwards.
  */
object AtTimeInput {

  /** What the field's current text amounts to. */
  sealed trait Resolved
  case object Live extends Resolved
  final case class Pinned(millis: Long) extends Resolved
  final case class Invalid(reason: String) extends Resolved

  /** A unit of offset. Weeks, months, and years are fixed spans (7, 30, and 365 days), the same
    * the ruler's ticks stand for, not calendar arithmetic: `-1mo` is thirty days back.
    */
  sealed abstract class OffsetUnit(val shorthand: String, val millis: Long)
  object OffsetUnit {
    case object Seconds extends OffsetUnit("s", 1000L)
    case object Minutes extends OffsetUnit("m", 60L * Seconds.millis)
    case object Hours extends OffsetUnit("h", 60L * Minutes.millis)
    case object Days extends OffsetUnit("d", 24L * Hours.millis)
    case object Weeks extends OffsetUnit("w", 7L * Days.millis)
    case object Months extends OffsetUnit("mo", 30L * Days.millis)
    case object Years extends OffsetUnit("y", 365L * Days.millis)

    /** Ascending by size. */
    val all: Seq[OffsetUnit] = Seq(Seconds, Minutes, Hours, Days, Weeks, Months, Years)

    /** The unit named by a word in the free-text grammar (`-15s`, `+2 hours`, `-1mo`). `m` is
      * minutes and `mo` months, as on the ruler.
      */
    def fromWord(word: String): Option[OffsetUnit] = word.toLowerCase match {
      case "s" | "sec" | "secs" | "second" | "seconds" => Some(Seconds)
      case "m" | "min" | "mins" | "minute" | "minutes" => Some(Minutes)
      case "h" | "hr" | "hrs" | "hour" | "hours" => Some(Hours)
      case "d" | "day" | "days" => Some(Days)
      case "w" | "wk" | "wks" | "week" | "weeks" => Some(Weeks)
      case "mo" | "mos" | "month" | "months" => Some(Months)
      case "y" | "yr" | "yrs" | "year" | "years" => Some(Years)
      case _ => None
    }
  }

  sealed abstract class OffsetDirection(val sign: Int, val symbol: String)
  object OffsetDirection {
    case object Before extends OffsetDirection(-1, "-")
    case object After extends OffsetDirection(1, "+")
    val all: Seq[OffsetDirection] = Seq(Before, After)

    def fromSymbol(symbol: String): Option[OffsetDirection] = all.find(_.symbol == symbol)
  }

  /** The text the field holds when the dialog opens: the moment currently tracked, so reopening
    * shows what is set and a tweak starts from it.
    */
  def initialText(current: Option[Long]): String = current.fold("now")(formatIso)

  /** One accepted format: examples of it, each of which the dialog offers as a click that puts
    * that text in the field, and what it means. The examples are the quick picks, so every one
    * names a moment that can be applied as it is: none lies in the future.
    */
  final case class Format(examples: Seq[String], meaning: String)

  val formats: Seq[Format] = Seq(
    Format(Seq("now"), "the present moment, tracked live"),
    Format(
      Seq("-15s", "-1m", "-1h", "-1d", "-1w", "-1mo", "-1y", "-1h30m"),
      "this far before now, resolved when you press Set, terms add up",
    ),
    Format(Seq("six seconds ago", "today", "yesterday"), "a phrase, today and yesterday meaning the start of that day"),
    Format(Seq("2021-05-29T10:02:00.004Z"), "an ISO 8601 instant"),
    Format(Seq("6:47 PM December 21, 2021"), "any parseable date string"),
    Format(Seq("1622282520004"), "milliseconds since the Unix epoch"),
  )

  /** One amount of one unit; an offset is a sum of these (`-1h30m` is two). */
  final case class OffsetTerm(amount: String, unit: OffsetUnit)

  def resolveOffset(terms: Seq[OffsetTerm], direction: OffsetDirection, nowMillis: Long): Resolved = {
    val amounts = terms.map(t => Try(t.amount.trim.toDouble).toOption.filter(n => n >= 0 && !n.isInfinite))
    if (terms.nonEmpty && amounts.forall(_.isDefined)) {
      val total = amounts.flatten.zip(terms).map { case (n, t) => n * t.unit.millis.toDouble }.sum
      Pinned(nowMillis + direction.sign * math.round(total))
    } else Invalid("Enter a non-negative amount.")
  }

  /** `-15s`, `+ 2 hours`, `-1h30m`, `- 1 year 1 minute 1 second`: a sign, then one or more
    * amounts each with a unit, added together.
    */
  private object Shorthand {
    private val Pattern: Regex = """([\-+])\s*((?:\d+\.?\d*\s*[A-Za-z]+\s*)+)""".r
    private val Term: Regex = """(\d+\.?\d*)\s*([A-Za-z]+)""".r

    def unapply(text: String): Option[(OffsetDirection, Seq[OffsetTerm])] = text match {
      case Pattern(symbol, body) =>
        val terms = Term.findAllMatchIn(body).map(m => OffsetUnit.fromWord(m.group(2)).map(OffsetTerm(m.group(1), _)))
        for {
          direction <- OffsetDirection.fromSymbol(symbol)
          known <- terms.foldLeft(Option(Vector.empty[OffsetTerm]))((acc, t) => acc.flatMap(v => t.map(v :+ _)))
        } yield (direction, known)
      case _ => None
    }
  }

  private object EpochMillis {
    def unapply(text: String): Option[Long] = Try(text.toLong).toOption
  }

  /** The free-text grammar the old prompt accepted, in the same order of precedence: `now` (in
    * any case), epoch milliseconds, an offset shorthand, then anything Sugar can read as a date
    * ("six seconds ago", "6:47 PM December 21, 2043", an ISO instant).
    *
    * The result is then held to the span anything can have been recorded in, from the Unix
    * epoch up to `nowMillis` ([[recorded]]).
    *
    * `nowMillis` is read once, after the text is parsed. Sugar timestamps a phrase that means the
    * present ("right now") as it parses it, so a clock read before the parse could sit a moment
    * behind that instant and the present would be refused as the future.
    */
  def resolveText(text: String, nowMillis: => Long): Resolved = {
    val parsed: Long => Resolved = text.trim match {
      case "" => _ => Invalid("Enter a moment in time.")
      case word if word.equalsIgnoreCase("now") => _ => Live
      case EpochMillis(millis) => _ => Pinned(millis)
      case Shorthand(direction, terms) => now => resolveOffset(terms, direction, now)
      case other =>
        val sugarDate = Sugar.Date.create(other)
        if (Sugar.Date.isValid(sugarDate)) {
          val millis = sugarDate.getTime().toLong
          _ => Pinned(millis)
        } else (_ => Invalid(s""""$other" is not a recognizable moment."""))
    }
    val now = nowMillis
    recorded(now)(parsed(now))
  }

  /** The earliest instant a pin may name: the Unix epoch. Nothing has been recorded before it,
    * and a `Long` far outside the range of dates cannot even be formatted as one.
    */
  val EarliestMillis = 0L

  /** Refuses a pinned instant outside the span anything can have been recorded in.
    *
    * After `nowMillis` the API would accept it, but a future pin only masquerades as the present:
    * every node it touches is read-only, standing queries stop syncing, and once the clock
    * passes it the same setting silently becomes a frozen past. Before the epoch nothing exists
    * either, and far enough out the instant is not a date at all.
    */
  private def recorded(nowMillis: Long)(resolved: Resolved): Resolved = resolved match {
    case Pinned(millis) if millis > nowMillis =>
      Invalid(s"${describeRelative(millis, nowMillis).capitalize} is in the future, nothing has been recorded there.")
    case Pinned(millis) if millis < EarliestMillis =>
      Invalid("That is before 1970, nothing has been recorded there.")
    case other => other
  }

  /** The shorthand naming an instant's offset from now, snapped to a whole number of the most
    * readable unit: `-15s`, `-3h`, `+2d`. The ruler writes this into the field, so a drag lands on
    * text the user could have typed and the grammar above stays the only parser.
    *
    * The offset is first rounded in the unit its size calls for (seconds under two minutes, minutes
    * under two hours, hours under two days, then days), then named in the largest unit that
    * divides it evenly, so a click on the ruler's `1h` tick writes `-1h` rather than `-60m`, and
    * its `1w`, `1mo`, and `1y` ticks write `-1w`, `-1mo`, and `-1y`.
    */
  def shorthand(millis: Long, nowMillis: Long): String = {
    import OffsetUnit._
    val delta = millis - nowMillis
    val distance = math.abs(delta)
    val roundingUnit =
      if (distance < 2L * Minutes.millis) Seconds
      else if (distance < 2L * Hours.millis) Minutes
      else if (distance < 2L * Days.millis) Hours
      else Days
    val snapped = math.max(1L, math.round(distance.toDouble / roundingUnit.millis)) * roundingUnit.millis
    val unit = all.reverse.find(u => snapped % u.millis == 0L).getOrElse(Seconds)
    // Zero rounds toward the past: a second ago has data, a second from now does not.
    val direction = if (delta <= 0) OffsetDirection.Before else OffsetDirection.After
    s"${direction.symbol}${snapped / unit.millis}${unit.shorthand}"
  }

  // ── Formatting ───────────────────────────────────────────────────────────────

  def formatIso(millis: Long): String = new js.Date(millis.toDouble).toISOString()

  private lazy val localFormatter: js.Dynamic =
    js.Dynamic.global.Intl.DateTimeFormat(
      js.undefined,
      js.Dynamic.literal(
        year = "numeric",
        month = "short",
        day = "numeric",
        hour = "numeric",
        minute = "2-digit",
        second = "2-digit",
        timeZoneName = "short",
      ),
    )

  /** The instant in the viewer's locale and zone, e.g. `Sep 10, 2026, 11:30:51 AM EDT`. */
  def formatLocal(millis: Long): String =
    localFormatter.format(new js.Date(millis.toDouble)).asInstanceOf[String]

  /** "15 seconds ago", "in 3 hours", "just now": how far the instant is from now. */
  def describeRelative(millis: Long, nowMillis: Long): String = {
    val delta = millis - nowMillis
    val distance = math.abs(delta)
    def count(n: Long, unit: String): String = s"$n $unit${if (n == 1) "" else "s"}"
    val phrase =
      if (distance < 1500L) None
      else if (distance < OffsetUnit.Minutes.millis) Some(count(math.round(distance / 1000.0), "second"))
      else if (distance < OffsetUnit.Hours.millis)
        Some(count(math.round(distance.toDouble / OffsetUnit.Minutes.millis), "minute"))
      else if (distance < OffsetUnit.Days.millis)
        Some(count(math.round(distance.toDouble / OffsetUnit.Hours.millis), "hour"))
      else if (distance < 30L * OffsetUnit.Days.millis)
        Some(count(math.round(distance.toDouble / OffsetUnit.Days.millis), "day"))
      else if (distance < 365L * OffsetUnit.Days.millis)
        Some(count(math.round(distance.toDouble / (30L * OffsetUnit.Days.millis)), "month"))
      else Some(count(math.round(distance.toDouble / (365L * OffsetUnit.Days.millis)), "year"))
    phrase.fold("just now")(p => if (delta < 0) s"$p ago" else s"in $p")
  }
}
