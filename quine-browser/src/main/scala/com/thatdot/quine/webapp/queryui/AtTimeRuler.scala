package com.thatdot.quine.webapp.queryui

/** Geometry of the query-time dialog's distance ruler: a fixed strip, ending at now, that shows
  * how far back the entered moment lies. Pure and in viewBox units, so the mapping between a
  * position and an offset is unit tested without a DOM; [[AtTimeModal]] draws it.
  *
  * The strip runs leftward from `NowX` on a log scale, a quarter second at the near end and ten
  * years at the far edge, so each tick to the left is roughly ten times further back and any
  * past offset the grammar can produce lands somewhere on it (clamped at the far edge). There
  * is nothing to the right of now: the future holds no recorded data and the dialog rejects it,
  * so no position on the strip can name it.
  */
object AtTimeRuler {

  val Width = 540.0

  /** Tall enough for the chosen marker's bubble, which sits 24 below the baseline and is 18
    * high with a one-unit stroke, plus a little clearance.
    */
  val Height = 92.0

  /** The far edge of the strip, ten years back. Not `Left`, which would shadow `scala.util.Left`
    * wherever this object is imported.
    */
  val LeftEdge = 14.0
  val NowX = 500.0
  val Baseline = 46.0

  private val Second = 1000L
  private val Minute = 60L * Second
  private val Hour = 60L * Minute
  private val Day = 24L * Hour
  private val Week = 7L * Day
  private val Month = 30L * Day
  private val Year = 365L * Day

  /** The near end of the strip stands for a quarter second back, so the 1s tick clears the now
    * dot and a position can never name an offset of zero: "now" is chosen by typing it.
    */
  val MinMillis: Double = Second / 4.0
  val MaxMillis: Double = (10L * Year).toDouble

  /** A labelled offset on the strip. Major ticks carry a longer mark. */
  final case class Tick(deltaMillis: Long, label: String, major: Boolean) {
    def x: Double = xOf(deltaMillis)
  }

  val ticks: Seq[Tick] = Seq(
    Tick(-Second, "1s", major = true),
    Tick(-10L * Second, "10s", major = false),
    Tick(-Minute, "1m", major = true),
    Tick(-10L * Minute, "10m", major = false),
    Tick(-Hour, "1h", major = true),
    Tick(-6L * Hour, "6h", major = false),
    Tick(-Day, "1d", major = true),
    Tick(-Week, "1w", major = false),
    Tick(-Month, "1mo", major = true),
    Tick(-Year, "1y", major = true),
    Tick(-10L * Year, "10y", major = false),
  )

  /** How far along the strip (0 at now, 1 at the far edge) a distance back from now sits. */
  private def fraction(distanceMillis: Double): Double = {
    val f = math.log10(math.max(distanceMillis, MinMillis) / MinMillis) / math.log10(MaxMillis / MinMillis)
    math.min(1.0, math.max(0.0, f))
  }

  /** Where an offset from now sits on the strip. An offset into the future sits at now, since
    * the strip has no future to place it in.
    */
  def xOf(deltaMillis: Long): Double =
    if (deltaMillis >= 0) NowX else NowX - fraction(-deltaMillis.toDouble) * (NowX - LeftEdge)

  /** The offset from now a position on the strip stands for: the inverse of [[xOf]] between
    * `LeftEdge` and `NowX`, never closer to now than a quarter second, never after it.
    */
  def deltaOf(x: Double): Double = {
    val cx = math.min(NowX, math.max(LeftEdge, x))
    -MinMillis * math.pow(MaxMillis / MinMillis, (NowX - cx) / (NowX - LeftEdge))
  }

  /** How close, in viewBox units, a pointer must be to a tick to land exactly on it. */
  val SnapDistance = 4.0

  /** The offset chosen by pointing at a position: a tick's own offset when within reach of one, so
    * clicking a label gives exactly that offset, and otherwise the position's own.
    */
  def snappedDeltaOf(x: Double): Long =
    ticks.find(t => math.abs(t.x - x) <= SnapDistance).fold(math.round(deltaOf(x)))(_.deltaMillis)

  /** Whether an offset lies beyond the far edge, in which case its marker sits pinned there. */
  def offStrip(deltaMillis: Long): Boolean = deltaMillis.toDouble < -MaxMillis
}
