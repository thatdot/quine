package com.thatdot.quine.webapp.components.landing

import scala.math.BigDecimal.RoundingMode

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.{MetricsReport, ShardInMemoryLimit}
import com.thatdot.quine.webapp.components.dashboard.{Card, ProgressBarMeter}
import com.thatdot.quine.webapp.util.Pot

/** Card displaying host memory metrics with progress bars. */
object HostMetricsCard {

  type MetricsData = (MetricsReport, Map[Int, ShardInMemoryLimit])

  val requiredPermissions: Set[String] = Set("ApplicationMetricsRead", "ShardLimitsRead")

  def apply(metricsSignal: Signal[Pot[MetricsData]]): HtmlElement =
    div(child <-- metricsSignal.map(renderState))

  private def renderState(state: Pot[MetricsData]): HtmlElement = state match {
    case Pot.Empty =>
      Card(title = "Host Metrics", body = p(cls := "text-muted mb-0", "No metrics data loaded."))

    case Pot.Pending =>
      Card(
        title = "Host Metrics",
        body = div(
          cls := "d-flex align-items-center",
          span(cls := "spinner-border spinner-border-sm me-2"),
          span("Loading metrics..."),
        ),
      )

    case Pot.Ready(data) =>
      renderContent(data)

    case Pot.Failed(err) =>
      Card(
        title = "Host Metrics",
        body = div(cls := "alert alert-danger mb-0", s"Error: $err"),
      )

    case Pot.PendingStale(data) =>
      div(
        cls := "position-relative",
        div(cls := "opacity-50", renderContent(data)),
        div(
          cls := "position-absolute top-0 start-0 w-100 text-center mt-4",
          span(cls := "spinner-border spinner-border-sm me-1"),
          span("Refreshing..."),
        ),
      )

    case Pot.FailedStale(data, err) =>
      div(
        div(cls := "alert alert-danger mb-2", s"Error refreshing: $err"),
        renderContent(data),
      )
  }

  private def renderContent(data: MetricsData): HtmlElement = {
    val (metrics, shards) = data
    val memoryBars = extractMemoryBars(metrics)
    val shardSummary = if (shards.nonEmpty) Seq(shardSummaryRow(shards)) else Seq.empty
    Card(
      title = "Host Metrics",
      body =
        if (memoryBars.nonEmpty || shardSummary.nonEmpty) div((memoryBars ++ shardSummary): _*)
        else p(cls := "text-muted mb-0", "No memory metrics available."),
    )
  }

  private def extractMemoryBars(metrics: MetricsReport): Seq[HtmlElement] = {
    val gauges = Seq(
      ("Total Memory", "Sum of on-heap JVM memory used across this host.", "memory.total.used", "memory.total.max"),
      ("Heap Usage", "Current heap usage vs. the configured maximum heap.", "memory.heap.used", "memory.heap.max"),
    )
    gauges.flatMap { case (title, description, currName, maxName) =>
      def normalizeMb(bytes: Double): Double = {
        val MB_IN_B = 1024 * 1024
        (BigDecimal(bytes) / MB_IN_B).setScale(3, RoundingMode.HALF_UP).toDouble
      }
      for {
        maxGauge <- metrics.gauges.find(_.name == maxName)
        if maxGauge.value > 0
        currGauge <- metrics.gauges.find(_.name == currName)
      } yield {
        val currBytes = currGauge.value
        val maxBytes = maxGauge.value
        val pct = if (maxBytes > 0) currBytes / maxBytes * 100.0 else 0.0
        val html = LandingTooltip.header(title) +
          s"""<div style="font-size: 13px; color: #acacc9; margin-bottom: 6px;">${LandingTooltip.escape(
            description,
          )}</div>""" +
          LandingTooltip.kvTable(
            LandingTooltip.kvRow("Current", formatBytes(currBytes)) +
            LandingTooltip.kvRow("Max", formatBytes(maxBytes)) +
            LandingTooltip.kvRow("Usage", f"$pct%.1f%%") +
            LandingTooltip.kvRow("Gauge", currName),
          )
        div(
          cls := "mb-2",
          styleAttr := "cursor: pointer;",
          LandingTooltip.attach[com.raquo.laminar.nodes.ReactiveHtmlElement.Base](html),
          small(cls := "text-muted", title),
          ProgressBarMeter(
            name = "MB",
            value = normalizeMb(currGauge.value),
            softMax = normalizeMb(maxGauge.value),
            hardMax = normalizeMb(maxGauge.value),
          ),
        )
      }
    }
  }

  private def shardSummaryRow(shards: Map[Int, ShardInMemoryLimit]): HtmlElement = {
    val totalSoft = shards.values.map(_.softLimit.toLong).sum
    val totalHard = shards.values.map(_.hardLimit.toLong).sum
    val sorted = shards.toSeq.sortBy(_._1)
    val items = sorted.map { case (id, lim) =>
      LandingTooltip.BreakdownItem(
        name = s"shard-$id",
        detail = s"soft ${lim.softLimit}  hard ${lim.hardLimit}",
      )
    }
    val html = LandingTooltip.header("In-memory shard limits") +
      s"""<div style="font-size: 13px; color: #acacc9; margin-bottom: 6px;">Per-shard thresholds for in-memory node counts. Soft = start evicting; hard = stop loading new nodes.</div>""" +
      LandingTooltip.kvTable(
        LandingTooltip.kvRow("Shards", s"${shards.size}") +
        LandingTooltip.kvRow("Total soft", s"$totalSoft nodes") +
        LandingTooltip.kvRow("Total hard", s"$totalHard nodes"),
      ) +
      LandingTooltip.subheader("Per-shard") +
      LandingTooltip.breakdownTable(items)

    div(
      cls := "mt-3 d-flex justify-content-between align-items-center",
      styleAttr := "cursor: pointer; font-size: 0.95em; color: var(--thatdot-brite-blue);",
      LandingTooltip.attach[com.raquo.laminar.nodes.ReactiveHtmlElement.Base](html),
      span(cls := "text-muted", s"${shards.size} shard${if (shards.size == 1) "" else "s"}"),
      span(s"soft $totalSoft / hard $totalHard nodes"),
    )
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
