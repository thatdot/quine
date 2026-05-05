package com.thatdot.quine.webapp.components.landing

import scala.math.BigDecimal.RoundingMode

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.{MetricsReport, ShardInMemoryLimit}
import com.thatdot.quine.webapp.components.dashboard.Card
import com.thatdot.quine.webapp.util.Pot

/** Card displaying host memory metrics with progress bars.
  *
  * The card and its rows are stable elements. Progress bar widths and labels are
  * driven by signals so the same DOM nodes are reused across polls. Reusing the
  * `progress-bar-striped` element matters in particular because Bootstrap's CSS
  * animation on that class restarts whenever the element is re-attached, which
  * was the source of the frame-long flicker on every refresh.
  */
object HostMetricsCard {

  type MetricsData = (MetricsReport, Map[Int, ShardInMemoryLimit])

  val requiredPermissions: Set[String] = Set("ApplicationMetricsRead", "ShardLimitsRead")

  final private case class GaugeView(currentMb: Double, maxMb: Double, currentBytes: Double, maxBytes: Double)

  def apply(metricsSignal: Signal[Pot[MetricsData]]): HtmlElement = {
    // Per-field signals, all `.distinct` so a poll that returns identical numbers
    // doesn't even touch the DOM. This is the canonical Laminar pattern: bind the
    // smallest piece, dedupe at that level, leave the surrounding structure mounted.
    val totalMemSig: Signal[Option[GaugeView]] =
      metricsSignal
        .map(_.toOption.flatMap { case (m, _) => extractGauge(m, "memory.total.used", "memory.total.max") })
        .distinct
    val heapSig: Signal[Option[GaugeView]] =
      metricsSignal
        .map(_.toOption.flatMap { case (m, _) => extractGauge(m, "memory.heap.used", "memory.heap.max") })
        .distinct
    val shardsSig: Signal[Map[Int, ShardInMemoryLimit]] =
      metricsSignal.map(_.toOption.map(_._2).getOrElse(Map.empty)).distinct
    val statusSig: Signal[CardStatus] = metricsSignal.map {
      case Pot.Empty => CardStatus.Empty
      case Pot.Pending => CardStatus.Pending
      case Pot.Failed(err) => CardStatus.Failed(err)
      case _ => CardStatus.Loaded
    }.distinct

    Card(
      title = "Host Metrics",
      body = div(
        // Status banner / placeholder. `statusSig` is `.distinct`, so the mapped
        // element is only rebuilt on a real status transition (Loaded → Pending → ...),
        // not on every poll.
        child.maybe <-- statusSig.map {
          case CardStatus.Empty => Some(p(cls := "text-muted mb-0", "No metrics data loaded."))
          case CardStatus.Pending =>
            Some(
              div(
                cls := "d-flex align-items-center",
                span(cls := "spinner-border spinner-border-sm me-2"),
                span("Loading metrics..."),
              ),
            )
          case CardStatus.Failed(err) =>
            Some(div(cls := "alert alert-danger mb-0", s"Error: $err"))
          case CardStatus.Loaded => None
        },
        // Stable rows — hidden when the corresponding gauge is missing.
        gaugeRow("Total Memory", "Sum of on-heap JVM memory used across this host.", "memory.total.used", totalMemSig),
        gaugeRow("Heap Usage", "Current heap usage vs. the configured maximum heap.", "memory.heap.used", heapSig),
        shardSummaryRow(shardsSig),
      ),
    )
  }

  sealed private trait CardStatus
  private object CardStatus {
    case object Empty extends CardStatus
    case object Pending extends CardStatus
    case object Loaded extends CardStatus
    final case class Failed(err: String) extends CardStatus
  }

  /** Stable row for one gauge. The bar's label and width come from `gaugeSignal` —
    * each poll only updates two attributes (`width`, the label text), so the
    * `progress-bar-striped` element keeps its CSS animation phase intact.
    */
  private def gaugeRow(
    title: String,
    description: String,
    gaugeName: String,
    gaugeSignal: Signal[Option[GaugeView]],
  ): HtmlElement = {
    val labelSig: Signal[String] = gaugeSignal.map {
      case Some(g) => s"MB ${g.currentMb}/${g.maxMb}"
      case None => ""
    }.distinct
    val pctSig: Signal[String] = gaugeSignal.map {
      case Some(g) if g.maxMb > 0 =>
        s"${math.max(math.min(g.currentMb / g.maxMb, 1.0), 0.0) * 100}%"
      case _ => "0%"
    }.distinct
    val displaySig: Signal[String] = gaugeSignal.map(g => if (g.isEmpty) "none" else "").distinct
    div(
      cls := "mb-2",
      styleAttr := "cursor: pointer;",
      // Hide the row when no gauge data exists rather than removing it from the DOM —
      // re-adding it would restart Bootstrap's progress-bar animation
      display <-- displaySig,
      // The tooltip handler needs the current gauge value at hover time. Subscribe
      // to a `StrictSignal` on mount so `.now()` is safe to call from the imperative
      // tooltip-attach helper. This is the canonical Laminar pattern for "give me
      // the latest value in an event handler" — preferred to mirroring the signal
      // into a Var (Signal#observe yields a StrictSignal whose `.now()` is public).
      onMountCallback { ctx =>
        val strict = gaugeSignal.observe(ctx.owner)
        LandingTooltip.attachToElement(
          ctx.thisNode.ref,
          () =>
            strict.now() match {
              case Some(g) => buildGaugeTooltip(title, description, gaugeName, g)
              case None => ""
            },
        )
      },
      small(cls := "text-muted", title),
      div(
        cls := "p-2 d-flex flex-row",
        div(cls := "label me-3 align-self-center", text <-- labelSig),
        div(
          cls := "progress flex-grow-1",
          height := "30px",
          div(
            cls := "progress-bar progress-bar-striped",
            role := "progressbar",
            width <-- pctSig,
          ),
        ),
      ),
    )
  }

  /** Stable shard-summary row. Hidden when there are no shards. */
  private def shardSummaryRow(shardsSignal: Signal[Map[Int, ShardInMemoryLimit]]): HtmlElement = {
    val countText: Signal[String] =
      shardsSignal.map(s => s"${s.size} shard${if (s.size == 1) "" else "s"}").distinct
    val totalsText: Signal[String] = shardsSignal.map { s =>
      val totalSoft = s.values.map(_.softLimit.toLong).sum
      val totalHard = s.values.map(_.hardLimit.toLong).sum
      s"soft $totalSoft / hard $totalHard nodes"
    }.distinct
    val displaySig: Signal[String] = shardsSignal.map(s => if (s.isEmpty) "none" else "").distinct
    div(
      cls := "mt-3 d-flex justify-content-between align-items-center",
      styleAttr := "cursor: pointer; font-size: 0.95em; color: var(--thatdot-brite-blue);",
      display <-- displaySig,
      onMountCallback { ctx =>
        val strict = shardsSignal.observe(ctx.owner)
        LandingTooltip.attachToElement(
          ctx.thisNode.ref,
          () => buildShardTooltip(strict.now()),
        )
      },
      span(cls := "text-muted", text <-- countText),
      span(text <-- totalsText),
    )
  }

  private def extractGauge(metrics: MetricsReport, currName: String, maxName: String): Option[GaugeView] =
    for {
      maxGauge <- metrics.gauges.find(_.name == maxName)
      if maxGauge.value > 0
      currGauge <- metrics.gauges.find(_.name == currName)
    } yield {
      val MB_IN_B = 1024 * 1024
      def normalizeMb(bytes: Double): Double =
        (BigDecimal(bytes) / MB_IN_B).setScale(0, RoundingMode.HALF_UP).toDouble
      GaugeView(
        currentMb = normalizeMb(currGauge.value),
        maxMb = normalizeMb(maxGauge.value),
        currentBytes = currGauge.value,
        maxBytes = maxGauge.value,
      )
    }

  private def buildGaugeTooltip(title: String, description: String, gaugeName: String, g: GaugeView): String = {
    val pct = if (g.maxBytes > 0) g.currentBytes / g.maxBytes * 100.0 else 0.0
    LandingTooltip.header(title) +
    s"""<div style="font-size: 13px; color: #acacc9; margin-bottom: 6px;">${LandingTooltip.escape(
      description,
    )}</div>""" +
    LandingTooltip.kvTable(
      LandingTooltip.kvRow("Current", formatBytes(g.currentBytes)) +
      LandingTooltip.kvRow("Max", formatBytes(g.maxBytes)) +
      LandingTooltip.kvRow("Usage", f"$pct%.1f%%") +
      LandingTooltip.kvRow("Gauge", gaugeName),
    )
  }

  private def buildShardTooltip(shards: Map[Int, ShardInMemoryLimit]): String = {
    if (shards.isEmpty) return ""
    val totalSoft = shards.values.map(_.softLimit.toLong).sum
    val totalHard = shards.values.map(_.hardLimit.toLong).sum
    val sorted = shards.toSeq.sortBy(_._1)
    val items = sorted.map { case (id, lim) =>
      LandingTooltip.BreakdownItem(
        name = s"shard-$id",
        detail = s"soft ${lim.softLimit}  hard ${lim.hardLimit}",
      )
    }
    LandingTooltip.header("In-memory shard limits") +
    s"""<div style="font-size: 13px; color: #acacc9; margin-bottom: 6px;">Per-shard thresholds for in-memory node counts. Soft = start evicting; hard = stop loading new nodes.</div>""" +
    LandingTooltip.kvTable(
      LandingTooltip.kvRow("Shards", s"${shards.size}") +
      LandingTooltip.kvRow("Total soft", s"$totalSoft nodes") +
      LandingTooltip.kvRow("Total hard", s"$totalHard nodes"),
    ) +
    LandingTooltip.subheader("Per-shard") +
    LandingTooltip.breakdownTable(items)
  }

  private def formatBytes(bytes: Double): String = {
    val KB = 1024.0
    val MB = KB * 1024
    val GB = MB * 1024
    if (bytes >= GB) f"${bytes / GB}%.2f GB"
    else if (bytes >= MB) f"${bytes / MB}%.1f MB"
    else if (bytes >= KB) f"${bytes / KB}%.0f KB"
    else f"${bytes.toLong}%,d B"
  }
}
