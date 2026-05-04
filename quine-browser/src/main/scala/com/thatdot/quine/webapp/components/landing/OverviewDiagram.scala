package com.thatdot.quine.webapp.components.landing

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.webapp.components.D3

/** Full-width system overview flow diagram rendered with D3.js.
  *
  * Shows: Ingest sources (left) → Quine (center) → Output destinations (right)
  * with a Persistor below center. Lines are bezier curves with thickness
  * proportional to aggregate throughput. Animated icons spawn periodically
  * along the lines based on rate.
  */
object OverviewDiagram {

  /** Aggregated node for display in the diagram. */
  private case class FlowNode(
    label: String,
    nodeType: String, // "kafka", "s3", "kinesis", "http", "stdout", "slack", etc.
    aggregateRate: Double,
    status: String, // "Running", "Mixed", "Failed", "Paused"
    count: Int, // number of individual ingests/outputs aggregated
  )

  def apply(
    ingests: Seq[IngestInfo],
    queries: Seq[StandingQueryInfo],
    persistor: PersistorInfo,
    clusterFullyUp: Option[Boolean] = None,
  ): HtmlElement = {
    // One node per ingest — intentionally not aggregated by source so users can
    // distinguish individual streams even when several share the same connection.
    val ingestNodes: Seq[FlowNode] = ingests
      .map { ingest =>
        FlowNode(ingest.name, ingest.sourceType, ingest.rate, ingest.status, count = 1)
      }
      .sortBy(-_.aggregateRate)

    // Separate cypher outputs (loop back to Quine) from regular outputs
    val allOutputs = queries.flatMap(_.outputs)
    val (cypherOutputs, regularOutputs) = allOutputs.partition(_.outputType == "cypher")

    val cypherNode: Option[FlowNode] = if (cypherOutputs.nonEmpty) {
      val totalRate = cypherOutputs.map(_.rate).sum
      Some(FlowNode("Cypher Queries", "cypher", totalRate, "Running", cypherOutputs.size))
    } else None

    val outputNodes: Seq[FlowNode] = regularOutputs
      .groupBy(_.destination)
      .toSeq
      .map { case (dest, group) =>
        val totalRate = group.map(_.rate).sum
        FlowNode(dest, group.head.outputType, totalRate, "Running", group.size)
      }
      .sortBy(-_.aggregateRate)

    // Track D3 intervals so we can stop them on re-render or unmount.
    val intervals = scala.collection.mutable.ArrayBuffer[js.Dynamic]()
    // ResizeObserver handle so unmount can disconnect it.
    var observer: js.Dynamic = null
    // Debounce rapid width changes during drag-resize.
    var rerenderTimeout: Int = 0
    var lastRenderedWidth: Int = -1

    def clearAll(container: dom.HTMLElement): Unit = {
      intervals.foreach(_.stop())
      intervals.clear()
      container.innerHTML = ""
    }

    def rerender(container: dom.HTMLElement): Unit = {
      clearAll(container)
      val newIntervals = renderDiagram(
        container,
        ingestNodes,
        outputNodes,
        cypherNode,
        persistor,
        ingests,
        queries,
        cypherOutputs,
        clusterFullyUp,
      )
      intervals ++= newIntervals
      lastRenderedWidth = container.clientWidth.toInt
    }

    div(
      width := "100%",
      onMountCallback { ctx =>
        val container = ctx.thisNode.ref
        rerender(container)
        val ro = js.Dynamic.global.ResizeObserver
        if (!js.isUndefined(ro)) {
          observer = js.Dynamic.newInstance(ro)({ (_: js.Any) =>
            val w = container.clientWidth.toInt
            if (math.abs(w - lastRenderedWidth) > 8) {
              if (rerenderTimeout != 0) dom.window.clearTimeout(rerenderTimeout)
              rerenderTimeout = dom.window.setTimeout(() => rerender(container), 120)
            }
          }: js.Function1[js.Any, Unit])
          val _ = observer.observe(container)
        }
      },
      onUnmountCallback { _ =>
        intervals.foreach(_.stop())
        intervals.clear()
        if (observer != null) observer.disconnect()
        if (rerenderTimeout != 0) dom.window.clearTimeout(rerenderTimeout)
      },
    )
  }

  private def renderDiagram(
    container: dom.HTMLElement,
    ingestNodes: Seq[FlowNode],
    outputNodes: Seq[FlowNode],
    cypherNode: Option[FlowNode],
    persistor: PersistorInfo,
    allIngests: Seq[IngestInfo],
    allQueries: Seq[StandingQueryInfo],
    cypherOutputs: Seq[StandingQueryOutputInfo],
    clusterFullyUp: Option[Boolean],
  ): Seq[js.Dynamic] = {
    // Ingest-name → its info (for tooltips). Each ingest is its own overview node now,
    // so the "members" list is typically size 1.
    val ingestsBySource: Map[String, Seq[IngestInfo]] = allIngests.groupBy(_.name)
    case class RawOutput(queryName: String, workflowName: String, outputType: String, rate: Double, totalCount: Long)
    val outputsByDest: Map[String, Seq[RawOutput]] =
      allQueries
        .flatMap(q =>
          q.outputs
            .filterNot(_.outputType == "cypher")
            .map(o => RawOutput(q.name, o.name, o.outputType, o.rate, o.totalCount) -> o.destination),
        )
        .groupBy(_._2)
        .view
        .mapValues(_.map(_._1))
        .toMap
    val cypherRawOutputs: Seq[RawOutput] =
      allQueries.flatMap(q =>
        q.outputs
          .filter(_.outputType == "cypher")
          .map(o => RawOutput(q.name, o.name, o.outputType, o.rate, o.totalCount)),
      )
    val _ = cypherOutputs // reserved for future

    val intervals = scala.collection.mutable.ArrayBuffer[js.Dynamic]()
    // Layout constants
    val width = container.clientWidth.toInt
    val nodeW = 130.0 // kept for layout reference
    val nodeH = 50.0 // kept for layout reference
    val cypherSpace = if (cypherNode.isDefined) 70 else 0
    val minNodeAreaHeight = 100
    val nodeAreaHeight = math.max(math.max(ingestNodes.size, outputNodes.size) * 95 + 60, minNodeAreaHeight)
    val persistorSpace = 80
    val height = cypherSpace + nodeAreaHeight + persistorSpace

    val leftX = 90.0
    val centerX = width / 2.0
    val rightX = width - 90.0
    val nodeRadius = 28.0 // center Quine node radius

    // Create SVG via D3
    val svg = D3
      .select(container)
      .append("svg")
      .attr("width", width)
      .attr("height", height)
      .attr("viewBox", s"0 0 $width $height")

    // Add a defs section for filters (the prototype uses no arrowheads — flow direction
    // is shown by the animated dot packets along each line).
    val defs = svg.append("defs")

    // Drop shadow filter for nodes
    val shadow = defs
      .append("filter")
      .attr("id", "node-shadow")
      .attr("x", "-10%")
      .attr("y", "-10%")
      .attr("width", "130%")
      .attr("height", "130%")
    shadow
      .append("feDropShadow")
      .attr("dx", 0)
      .attr("dy", 1)
      .attr("stdDeviation", 2)
      .attr("flood-opacity", 0.15)

    // Compute vertical positions for nodes (offset by cypherSpace)
    val ingestYs = computeNodeYs(ingestNodes.size, nodeAreaHeight).map(_ + cypherSpace)
    val outputYs = computeNodeYs(outputNodes.size, nodeAreaHeight).map(_ + cypherSpace)
    val quineY = cypherSpace + nodeAreaHeight / 2.0
    val persistorY = cypherSpace + nodeAreaHeight + 50.0

    // Per-side stroke scales: the ingest column has its own total/max, as does the
    // output column. This avoids one side with many small flows looking thin compared
    // to the other side with few big flows.
    val ingestTotal = math.max(ingestNodes.map(_.aggregateRate).sum, 1.0)
    val outputTotal = math.max(outputNodes.map(_.aggregateRate).sum, 1.0)

    val ingestStroke = D3
      .scaleLinear()
      .domain(js.Array(0.0, ingestTotal))
      .range(js.Array(1.5, 18.0))
    val outputStroke = D3
      .scaleLinear()
      .domain(js.Array(0.0, outputTotal))
      .range(js.Array(1.5, 18.0))

    // Target "visible dots" scaled so even a modest share feels lively. Using sqrt(share)
    // means a line with 4% of side-total flow gets ~20% of max dots — not just 4%.
    // Dots take ~2300ms to cross the line, so spawnInterval = duration / targetDots.
    val avgDotDuration = 2300.0
    val minVisibleDots = 3.0
    val maxVisibleDots = 14.0
    def spawnIntervalForShare(share: Double): Int = {
      val s = math.sqrt(math.min(math.max(share, 0.0), 1.0))
      val targetDots = minVisibleDots + s * (maxVisibleDots - minVisibleDots)
      math.max(100, (avgDotDuration / targetDots).toInt)
    }
    def ingestSpawn(rate: Double): Int = spawnIntervalForShare(rate / ingestTotal)
    def outputSpawn(rate: Double): Int = spawnIntervalForShare(rate / outputTotal)

    // Draw ingest links (left → center). Color reflects the ingest's status so a
    // paused/failed stream is visible at a glance.
    ingestNodes.zipWithIndex.foreach { case (node, idx) =>
      val y = ingestYs(idx)
      val strokeW = ingestStroke(node.aggregateRate)
      val pathData = bezierH(leftX + 18, y, centerX - nodeRadius, quineY)
      val lineColor = ingestFlowColor(node.status)

      svg
        .append("path")
        .attr("d", pathData)
        .attr("fill", "none")
        .attr("stroke", lineColor)
        .attr("stroke-width", strokeW)
        .attr("stroke-opacity", 0.35)

      // Spawn animated icons periodically
      if (node.aggregateRate > 0) {
        val intervalMs = ingestSpawn(node.aggregateRate)
        intervals += spawnAnimatedIcons(svg, pathData, intervalMs, lineColor)
      }

      // Hover overlay for this flow line
      val flowPath = svg
        .append("path")
        .attr("d", pathData)
        .attr("fill", "none")
        .attr("stroke", "transparent")
        .attr("stroke-width", math.max(20.0, strokeW.asInstanceOf[Double] + 10.0))
        .attr("pointer-events", "stroke")
        .attr("style", "cursor: pointer;")
      val members = ingestsBySource.getOrElse(node.label, Nil)
      LandingTooltip.attachToPath(
        flowPath.node().asInstanceOf[dom.Element],
        () => buildIngestFlowTooltip(node, members),
      )
    }

    // Draw output links (center → right)
    outputNodes.zipWithIndex.foreach { case (node, idx) =>
      val y = outputYs(idx)
      val strokeW = outputStroke(node.aggregateRate)
      val pathData = bezierH(centerX + nodeRadius, quineY, rightX - 18, y)

      svg
        .append("path")
        .attr("d", pathData)
        .attr("fill", "none")
        .attr("stroke", "#1658b7")
        .attr("stroke-width", strokeW)
        .attr("stroke-opacity", 0.35)

      // Spawn animated icons periodically
      if (node.aggregateRate > 0) {
        val intervalMs = outputSpawn(node.aggregateRate)
        intervals += spawnAnimatedIcons(svg, pathData, intervalMs, "#1658b7")
      }

      // Hover overlay for this flow line
      val flowPath = svg
        .append("path")
        .attr("d", pathData)
        .attr("fill", "none")
        .attr("stroke", "transparent")
        .attr("stroke-width", math.max(20.0, strokeW.asInstanceOf[Double] + 10.0))
        .attr("pointer-events", "stroke")
        .attr("style", "cursor: pointer;")
      val raws = outputsByDest.getOrElse(node.label, Nil)
      val items = raws
        .sortBy(-_.rate)
        .map(r =>
          LandingTooltip.BreakdownItem(s"${r.queryName} → ${r.workflowName}", LandingTooltip.formatRate(r.rate)),
        )
      val html = LandingTooltip.header(node.label) +
        LandingTooltip.kvTable(
          LandingTooltip.kvRow("Destination type", ServiceIcons.labelFor(node.nodeType)) +
          LandingTooltip.kvRow("Aggregate rate", LandingTooltip.formatRate(node.aggregateRate)) +
          LandingTooltip.kvRow("Total results", f"${raws.map(_.totalCount).sum}%,d") +
          LandingTooltip.kvRow("Workflows", s"${raws.size}") +
          LandingTooltip.kvRow("Queries", s"${raws.map(_.queryName).distinct.size}"),
        ) +
        (if (raws.nonEmpty)
           LandingTooltip.subheader("Queries feeding this destination") +
           LandingTooltip.breakdownTable(items)
         else "")
      LandingTooltip.attachToPath(
        flowPath.node().asInstanceOf[dom.Element],
        () => html,
      )
    }

    // Draw persistor links: two parallel lines between Quine and the persistor.
    //   Left line = writes (Quine → persistor), dots travel down.
    //   Right line = reads (persistor → Quine), dots travel up — achieved by defining
    //   the path from persistor to Quine so the `t=0→1` tween walks the reading direction.
    //
    // Line color encodes *latency health*:
    //   blue  = healthy (low latency)
    //   amber = degraded (p90-ish)
    //   red   = bad (p99-ish+)
    //
    // Dot volume encodes *throughput* (ops/sec), dot speed encodes *latency* within the
    // visible animation window.
    val persistorTopY = quineY + nodeRadius
    val persistorBotY = persistorY - 20
    val persistorLaneGap = 7.0
    val writeX = centerX - persistorLaneGap
    val readX = centerX + persistorLaneGap

    val writePathData = s"M$writeX,$persistorTopY L$writeX,$persistorBotY"
    // Defined persistor → Quine so dots animate bottom-to-top (read direction).
    val readPathData = s"M$readX,$persistorBotY L$readX,$persistorTopY"

    val writeColor = latencyHealthColor(persistor.writeLatencyMs)
    val readColor = latencyHealthColor(persistor.readLatencyMs)

    svg
      .append("path")
      .attr("d", writePathData)
      .attr("fill", "none")
      .attr("stroke", writeColor)
      .attr("stroke-width", persistorStrokeWidth(persistor.writeOpsPerSec))
      .attr("stroke-opacity", 0.5)

    svg
      .append("path")
      .attr("d", readPathData)
      .attr("fill", "none")
      .attr("stroke", readColor)
      .attr("stroke-width", persistorStrokeWidth(persistor.readOpsPerSec))
      .attr("stroke-opacity", 0.5)

    // Animated dots on each line.
    if (persistor.writeOpsPerSec > 0.01 || persistor.writeLatencyMs > 0.01) {
      val (intervalMs, durationMs) = persistorAnimationTiming(
        persistor.writeOpsPerSec,
        persistor.writeLatencyMs,
      )
      intervals += spawnAnimatedIcons(svg, writePathData, intervalMs, writeColor, durationMs)
    }
    if (persistor.readOpsPerSec > 0.01 || persistor.readLatencyMs > 0.01) {
      val (intervalMs, durationMs) = persistorAnimationTiming(
        persistor.readOpsPerSec,
        persistor.readLatencyMs,
      )
      intervals += spawnAnimatedIcons(svg, readPathData, intervalMs, readColor, durationMs)
    }

    // Draw ingest nodes
    ingestNodes.zipWithIndex.foreach { case (node, idx) =>
      val y = ingestYs(idx)
      drawNode(svg, leftX, y, nodeW, nodeH, node)

      val members = ingestsBySource.getOrElse(node.label, Nil)
      // Hug the visible icon + label + rate footprint rather than the layout cell.
      // An oversized invisible rect would keep the tooltip open over whitespace.
      val hoverW = 90.0
      val hoverH = 80.0
      val zone = svg
        .append("rect")
        .attr("x", leftX - hoverW / 2)
        .attr("y", y - 24)
        .attr("width", hoverW)
        .attr("height", hoverH)
        .attr("fill", "transparent")
        .attr("pointer-events", "all")
        .attr("style", "cursor: pointer;")
      LandingTooltip.attachToElement(
        zone.node().asInstanceOf[dom.Element],
        () => buildIngestFlowTooltip(node, members),
      )
    }

    // Draw output nodes
    outputNodes.zipWithIndex.foreach { case (node, idx) =>
      val y = outputYs(idx)
      drawNode(svg, rightX, y, nodeW, nodeH, node)

      val raws = outputsByDest.getOrElse(node.label, Nil)
      val hoverW = 90.0
      val hoverH = 80.0
      val zone = svg
        .append("rect")
        .attr("x", rightX - hoverW / 2)
        .attr("y", y - 24)
        .attr("width", hoverW)
        .attr("height", hoverH)
        .attr("fill", "transparent")
        .attr("pointer-events", "all")
        .attr("style", "cursor: pointer;")
      val items = raws
        .sortBy(-_.rate)
        .map(r =>
          LandingTooltip.BreakdownItem(s"${r.queryName} → ${r.workflowName}", LandingTooltip.formatRate(r.rate)),
        )
      val html = LandingTooltip.header(node.label) +
        LandingTooltip.kvTable(
          LandingTooltip.kvRow("Destination type", ServiceIcons.labelFor(node.nodeType)) +
          LandingTooltip.kvRow("Aggregate rate", LandingTooltip.formatRate(node.aggregateRate)) +
          LandingTooltip.kvRow("Total results", f"${raws.map(_.totalCount).sum}%,d") +
          LandingTooltip.kvRow("Workflows", s"${raws.size}") +
          LandingTooltip.kvRow("Queries", s"${raws.map(_.queryName).distinct.size}"),
        ) +
        (if (raws.nonEmpty)
           LandingTooltip.subheader("Queries feeding this destination") +
           LandingTooltip.breakdownTable(items)
         else "")
      LandingTooltip.attachToElement(
        zone.node().asInstanceOf[dom.Element],
        () => html,
      )
    }

    // Draw center Quine node (dark-blue filled circle with the white Quine icon inside).
    // Grouped so we can `.raise()` it after each animated dot spawn — otherwise newly
    // appended <circle> packets paint on top of Quine as they enter/exit the center.
    val quineOuterR = nodeRadius + 6
    // Ring color reflects cluster health: brite-blue when fully up (or unknown, i.e.
    // OSS single-node with no cluster-status endpoint), red when degraded. The dark-navy
    // fill stays constant so the white Quine logo remains legible.
    val quineStroke = clusterFullyUp match {
      case Some(false) => "#dc3545"
      case _ => "#1658b7"
    }
    val quineGroup = svg.append("g").attr("class", "quine-center-node")
    quineGroup
      .append("circle")
      .attr("cx", centerX)
      .attr("cy", quineY)
      .attr("r", quineOuterR)
      .attr("fill", "#0a295b")
      .attr("stroke", quineStroke)
      .attr("stroke-width", 3)

    // Icon fits inside the circle with a little padding. Nudged 2px down-right to
    // visually center the asymmetric Quine glyph inside the ring.
    val quineIconSize = quineOuterR * 1.3
    quineGroup
      .append("image")
      .attr("href", ServiceIcons.quineIcon)
      .attr("x", centerX - quineIconSize / 2 + 2)
      .attr("y", quineY - quineIconSize / 2 + 2)
      .attr("width", quineIconSize)
      .attr("height", quineIconSize)
      .attr("preserveAspectRatio", "xMidYMid meet")

    // Hover zone over the Quine node
    val quineZone = svg
      .append("circle")
      .attr("cx", centerX)
      .attr("cy", quineY)
      .attr("r", quineOuterR + 4)
      .attr("fill", "transparent")
      .attr("pointer-events", "all")
      .attr("style", "cursor: pointer;")
    val totalInRate = allIngests.map(_.rate).sum
    val totalOutRate = outputNodes.map(_.aggregateRate).sum
    val cypherRate = cypherNode.map(_.aggregateRate).getOrElse(0.0)
    val quineHtml =
      LandingTooltip.header("Quine") +
      LandingTooltip.kvTable(
        LandingTooltip.kvRow("Persistor", ServiceIcons.labelFor(persistor.name)) +
        LandingTooltip.kvRow("Total ingest rate", LandingTooltip.formatRate(totalInRate)) +
        LandingTooltip.kvRow("Total output rate", LandingTooltip.formatRate(totalOutRate)) +
        (if (cypherRate > 0) LandingTooltip.kvRow("Cypher loopback rate", LandingTooltip.formatRate(cypherRate))
         else "") +
        LandingTooltip.kvRow("Sources", s"${ingestNodes.size}") +
        LandingTooltip.kvRow("Destinations", s"${outputNodes.size}"),
      )
    LandingTooltip.attachToElement(
      quineZone.node().asInstanceOf[dom.Element],
      () => quineHtml,
    )

    // Draw cypher loopback arc above Quine
    cypherNode.foreach { cNode =>
      val arcTop = quineY - nodeRadius - 50
      val arcPathData =
        s"M${centerX + nodeRadius},${quineY - 10} C${centerX + 80},$arcTop ${centerX - 80},$arcTop ${centerX - nodeRadius},${quineY - 10}"

      svg
        .append("path")
        .attr("d", arcPathData)
        .attr("fill", "none")
        .attr("stroke", "#1658b7")
        .attr("stroke-width", 2.5)
        .attr("stroke-opacity", 0.4)

      // Label at top of arc
      svg
        .append("text")
        .attr("x", centerX)
        .attr("y", arcTop - 5)
        .attr("text-anchor", "middle")
        .attr("font-size", "14px")
        .attr("font-weight", "500")
        .attr("fill", "#0a295b")
        .text(s"Cypher Queries")

      // Rate below the label
      if (cNode.aggregateRate > 0) {
        svg
          .append("text")
          .attr("x", centerX)
          .attr("y", arcTop + 13)
          .attr("text-anchor", "middle")
          .attr("font-size", "13px")
          .attr("fill", "#6c757d")
          .text(LandingTooltip.formatRate(cNode.aggregateRate))
      }

      // Count badge
      if (cNode.count > 1) {
        svg
          .append("circle")
          .attr("cx", centerX + 55)
          .attr("cy", arcTop - 5)
          .attr("r", 10)
          .attr("fill", "#1658b7")

        svg
          .append("text")
          .attr("x", centerX + 55)
          .attr("y", arcTop - 5)
          .attr("text-anchor", "middle")
          .attr("font-size", "11px")
          .attr("font-weight", "bold")
          .attr("fill", "white")
          .attr("dominant-baseline", "central")
          .text(cNode.count.toString)
      }

      // Animated icons along the arc (cypher loopback is routed on the output side)
      if (cNode.aggregateRate > 0) {
        val intervalMs = outputSpawn(cNode.aggregateRate)
        intervals += spawnAnimatedIcons(svg, arcPathData, intervalMs, "#1658b7")
      }

      // Hover overlay for the cypher arc
      val arcZone = svg
        .append("path")
        .attr("d", arcPathData)
        .attr("fill", "none")
        .attr("stroke", "transparent")
        .attr("stroke-width", 24)
        .attr("pointer-events", "stroke")
        .attr("style", "cursor: pointer;")
      val items = cypherRawOutputs
        .sortBy(-_.rate)
        .map(r =>
          LandingTooltip.BreakdownItem(s"${r.queryName} → ${r.workflowName}", LandingTooltip.formatRate(r.rate)),
        )
      val cypherHtml =
        LandingTooltip.header("Cypher loopback") +
        LandingTooltip.kvTable(
          LandingTooltip.kvRow("Total rate", LandingTooltip.formatRate(cNode.aggregateRate)) +
          LandingTooltip.kvRow("Total results", f"${cypherRawOutputs.map(_.totalCount).sum}%,d") +
          LandingTooltip.kvRow("Cypher outputs", s"${cypherRawOutputs.size}") +
          LandingTooltip.kvRow("Queries", s"${cypherRawOutputs.map(_.queryName).distinct.size}"),
        ) +
        (if (cypherRawOutputs.nonEmpty)
           LandingTooltip.subheader("Cypher outputs") +
           LandingTooltip.breakdownTable(items)
         else "")
      LandingTooltip.attachToPath(
        arcZone.node().asInstanceOf[dom.Element],
        () => cypherHtml,
      )
    }

    // Draw persistor node.
    //   - If we have an icon, show it standalone (most branded persistor icons already
    //     include the product name, so no label and no box are needed).
    //   - Otherwise, draw a rounded rect with the persistor name inside, outlined with
    //     a color that reflects persistor health.
    val persistorIconOpt = ServiceIcons.forType(persistor.name)
    persistorIconOpt match {
      case Some(iconUri) =>
        val iconSize = 48.0
        svg
          .append("image")
          .attr("href", iconUri)
          .attr("x", centerX - iconSize / 2)
          .attr("y", persistorY - iconSize / 2)
          .attr("width", iconSize)
          .attr("height", iconSize)
          .attr("preserveAspectRatio", "xMidYMid meet")
      case None =>
        val persistorColor = persistor.status match {
          case "Healthy" => "#1658b7"
          case "Degraded" => "#ffc107"
          case _ => "#dc3545"
        }
        val persistorWidth = 120.0
        val persistorHeight = 44.0
        svg
          .append("rect")
          .attr("x", centerX - persistorWidth / 2)
          .attr("y", persistorY - persistorHeight / 2)
          .attr("width", persistorWidth)
          .attr("height", persistorHeight)
          .attr("rx", 6)
          .attr("fill", "white")
          .attr("stroke", persistorColor)
          .attr("stroke-width", 2.5)
        svg
          .append("text")
          .attr("x", centerX)
          .attr("y", persistorY + 5)
          .attr("text-anchor", "middle")
          .attr("font-size", "14px")
          .attr("fill", "#0a295b")
          .text(persistor.name)
    }

    // Hover zone over the persistor
    val persistorZoneW = 140.0
    val persistorZoneH = 70.0
    val persistorZone = svg
      .append("rect")
      .attr("x", centerX - persistorZoneW / 2)
      .attr("y", persistorY - persistorZoneH / 2)
      .attr("width", persistorZoneW)
      .attr("height", persistorZoneH)
      .attr("fill", "transparent")
      .attr("pointer-events", "all")
      .attr("style", "cursor: pointer;")
    val persistorHtml = {
      def msRow(label: String, ms: Double): String =
        if (ms > 0) LandingTooltip.kvRow(label, f"$ms%.2f ms") else ""
      def rateRow(label: String, rate: Double): String =
        if (rate > 0) LandingTooltip.kvRow(label, f"$rate%.1f ops/s") else ""
      LandingTooltip.header("Persistor") +
      LandingTooltip.kvTable(
        LandingTooltip.kvRow("Store type", ServiceIcons.labelFor(persistor.name)) +
        LandingTooltip.kvRow("Status", persistor.status) +
        rateRow("Write throughput", persistor.writeOpsPerSec) +
        msRow("Write latency", persistor.writeLatencyMs) +
        rateRow("Read throughput", persistor.readOpsPerSec) +
        msRow("Read latency", persistor.readLatencyMs),
      )
    }
    LandingTooltip.attachToElement(
      persistorZone.node().asInstanceOf[dom.Element],
      () => persistorHtml,
    )

    intervals.toSeq
  }

  private def buildIngestFlowTooltip(node: FlowNode, members: Seq[IngestInfo]): String =
    members match {
      case Seq(only) =>
        LandingTooltip.header(node.label) +
          LandingTooltip.kvTable(
            LandingTooltip.kvRow("Source type", ServiceIcons.labelFor(node.nodeType)) +
            LandingTooltip.kvRow("Source", only.source) +
            LandingTooltip.kvRow("Rate", LandingTooltip.formatRate(only.rate)) +
            LandingTooltip.kvRow("Status", only.status, Some(LandingTooltip.statusColor(only.status))),
          )
      case _ =>
        val sorted = members.sortBy(-_.rate)
        val items = sorted.map(m => LandingTooltip.BreakdownItem(m.name, m.status, LandingTooltip.formatRate(m.rate)))
        LandingTooltip.header(node.label) +
        LandingTooltip.kvTable(
          LandingTooltip.kvRow("Source type", ServiceIcons.labelFor(node.nodeType)) +
          LandingTooltip.kvRow("Aggregate rate", LandingTooltip.formatRate(node.aggregateRate)) +
          LandingTooltip.kvRow("Streams", s"${node.count}") +
          LandingTooltip.kvRow("Status", node.status, Some(LandingTooltip.statusColor(node.status))),
        ) +
        (if (members.nonEmpty)
           LandingTooltip.subheader("Individual streams") +
           LandingTooltip.breakdownTable(items)
         else "")
    }

  private def computeNodeYs(count: Int, totalHeight: Int): Seq[Double] =
    if (count == 0) Seq.empty
    else if (count == 1) Seq(totalHeight / 2.0)
    else {
      val spacing = (totalHeight - 70.0) / (count - 1)
      (0 until count).map(i => 35.0 + i * spacing)
    }

  private def bezierH(x1: Double, y1: Double, x2: Double, y2: Double): String = {
    val midX = (x1 + x2) / 2.0
    s"M$x1,$y1 C$midX,$y1 $midX,$y2 $x2,$y2"
  }

  private def nodeColor(status: String): String = status match {
    case "Running" => "#1658b7"
    case "Paused" => "#1658b7"
    case "Mixed" => "#ffc107"
    case "Failed" => "#dc3545"
    case _ => "#acacc9"
  }

  /** Per-ingest flow-line color. Matches the IngestsCard status palette so a glance at
    * either card conveys the same information.
    */
  private def ingestFlowColor(status: String): String = status match {
    case "Running" => "#1658b7" // brand blue
    case "Paused" | "Restored" => "#d6a100" // amber
    case "Failed" => "#dc3545" // red
    case _ => "#1658b7"
  }

  private def drawNode(svg: js.Dynamic, x: Double, y: Double, nodeW: Double, nodeH: Double, node: FlowNode): Unit = {
    val color = nodeColor(node.status)
    val iconOpt = ServiceIcons.forType(node.nodeType)
    val iconSize = 32

    val g = svg.append("g")

    // Service icon centered at (x, y)
    iconOpt.foreach { iconUri =>
      g.append("image")
        .attr("href", iconUri)
        .attr("x", x - iconSize / 2)
        .attr("y", y - iconSize / 2)
        .attr("width", iconSize)
        .attr("height", iconSize)
    }

    // If no icon, draw a small colored circle as a fallback
    if (iconOpt.isEmpty) {
      g.append("circle")
        .attr("cx", x)
        .attr("cy", y)
        .attr("r", 14)
        .attr("fill", "white")
        .attr("stroke", color)
        .attr("stroke-width", 2)
    }

    // Label below the icon
    val label = if (node.label.length > 18) node.label.take(16) + "\u2026" else node.label
    g.append("text")
      .attr("x", x)
      .attr("y", y + iconSize / 2 + 16)
      .attr("text-anchor", "middle")
      .attr("font-size", "14px")
      .attr("font-weight", "500")
      .attr("fill", "#0a295b")
      .text(label)

    // Rate below the label
    if (node.aggregateRate > 0) {
      g.append("text")
        .attr("x", x)
        .attr("y", y + iconSize / 2 + 32)
        .attr("text-anchor", "middle")
        .attr("font-size", "13px")
        .attr("fill", "#6c757d")
        .text(LandingTooltip.formatRate(node.aggregateRate))
    }

    // Count badge (top-right of icon)
    if (node.count > 1) {
      g.append("circle")
        .attr("cx", x + iconSize / 2)
        .attr("cy", y - iconSize / 2)
        .attr("r", 10)
        .attr("fill", color)

      val _ = g
        .append("text")
        .attr("x", x + iconSize / 2)
        .attr("y", y - iconSize / 2)
        .attr("text-anchor", "middle")
        .attr("font-size", "11px")
        .attr("font-weight", "bold")
        .attr("fill", "white")
        .attr("dominant-baseline", "central")
        .text(node.count.toString)
    }
  }

  /** Map persistor write latency (ms) and throughput (ops/sec) to animation timing.
    *
    * Latency is perceived through dot *speed*: slower dots mean a slower (high-latency)
    * store. The real latency range is non-linear (sub-ms Cassandra vs. 10+ms networked
    * stores), so we map log-scaled latency into a fixed [800, 5000]ms travel time.
    *
    * Throughput is perceived through dot *volume*: more ops/sec spawns more dots. We
    * sqrt-compress so even a low-throughput line shows occasional activity rather than
    * going silent.
    */
  private def persistorAnimationTiming(opsPerSec: Double, latencyMs: Double): (Int, Double) = {
    val minDuration = 800.0
    val maxDuration = 5000.0
    // Latency clamps: 0.1ms (crazy-fast RocksDB) → 50ms (slow networked store).
    val clamped = math.min(math.max(latencyMs, 0.1), 50.0)
    val normLog = (math.log10(clamped) - math.log10(0.1)) / (math.log10(50.0) - math.log10(0.1))
    val duration = minDuration + normLog * (maxDuration - minDuration)

    val minTargetDots = 1.0
    val maxTargetDots = 12.0
    // Saturate the share at 500 ops/sec; beyond that the line is visually "full".
    val share = math.min(math.max(opsPerSec / 500.0, 0.0), 1.0)
    val targetDots = minTargetDots + math.sqrt(share) * (maxTargetDots - minTargetDots)
    val intervalMs = math.max(80, (duration / targetDots).toInt)

    (intervalMs, duration)
  }

  /** Color-code a persistor line by latency health. These thresholds are tuned for
    * typical Quine storage backends — sub-ms for hot RocksDB/Cassandra, a few ms for
    * healthy-but-busy, tens of ms when the store is struggling.
    *   - healthy (<5ms)   → brand blue
    *   - degraded (<20ms) → amber
    *   - bad (≥20ms)      → red
    * `0` (no data) is treated as healthy so an idle cluster doesn't look broken.
    */
  private def latencyHealthColor(latencyMs: Double): String =
    if (latencyMs <= 0.0) "#0a295b" // idle, use the neutral dark brand color
    else if (latencyMs < 5.0) "#1658b7" // healthy — thatdot brite-blue
    else if (latencyMs < 20.0) "#d6a100" // degraded — amber
    else "#dc3545" // bad — red

  /** Line thickness scales mildly with throughput so a busy persistor reads as thicker. */
  private def persistorStrokeWidth(opsPerSec: Double): Double = {
    val minW = 2.0
    val maxW = 8.0
    val share = math.min(math.max(opsPerSec / 500.0, 0.0), 1.0)
    minW + math.sqrt(share) * (maxW - minW)
  }

  private def spawnAnimatedIcons(
    svg: js.Dynamic,
    pathData: String,
    intervalMs: Int,
    color: String,
    durationMs: Double = 2300.0,
  ): js.Dynamic = {
    // Create a hidden path element for measuring
    val pathEl = svg
      .append("path")
      .attr("d", pathData)
      .attr("fill", "none")
      .attr("stroke", "none")

    D3.interval(
      { (_: Double) =>
        val pathNode = pathEl.node().asInstanceOf[dom.svg.Path]
        val totalLength = pathNode.getTotalLength()

        // Animated circle "data packet" that moves along the path.
        // Insert *before* the Quine center group so dots paint underneath Quine as they
        // enter/exit the center. Falls back to `.append` if the Quine group isn't present
        // (e.g. during initial render ordering).
        val iconRaw =
          if (
            !js.isUndefined(svg.select(".quine-center-node").node()) &&
            svg.select(".quine-center-node").node() != null
          ) {
            svg.insert("circle", ".quine-center-node")
          } else {
            svg.append("circle")
          }
        val icon = iconRaw
          .attr("r", 5)
          .attr("fill", color)
          .attr("opacity", 0.6)
          .attr("stroke", "white")
          .attr("stroke-width", 1)
          // Dots are decorative — never let them capture hover events. Without this,
          // a dot passing under the cursor fires mouseleave on the underlying line/node
          // hover zone, flickering the tooltip off and on as each dot crosses.
          .attr("pointer-events", "none")

        // Animate along path at constant speed. The crossing time is fixed (not
        // jittered) so successive dots on the same line travel at identical speeds —
        // otherwise a later dot with a shorter duration would overtake an earlier one.
        // Linear easing (not the default cubic) prevents dots from bunching up at the
        // start and end of the line.
        val _ = icon
          .transition()
          .duration(durationMs)
          .ease(D3.easeLinear)
          .attrTween(
            "transform",
            { () =>
              val interpolate: js.Function1[Double, String] = { (t: Double) =>
                val point = pathNode.getPointAtLength(t * totalLength)
                s"translate(${point.x},${point.y})"
              }
              interpolate
            }: js.Function0[js.Function1[Double, String]],
          )
          .on(
            "end",
            { () =>
              val _ = icon.remove(); ()
            }: js.Function0[Unit],
          )
      },
      intervalMs.toDouble,
    )
  }
}
