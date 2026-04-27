package com.thatdot.quine.webapp.components.landing

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.webapp.components.D3
import com.thatdot.quine.webapp.components.dashboard.Card
import com.thatdot.quine.webapp.components.landing.V2ApiTypes.V2StandingQueryInfo
import com.thatdot.quine.webapp.util.Pot

/** Sankey-style visualization for standing queries.
  *
  * Left side: one row per standing query, with a vertical bar proportional to match rate.
  * Right side: one row per destination (an output workflow in V2). Icon by first destination type.
  * Bezier curves connect each query to its outputs, with stroke width proportional to rate.
  * Destinations are barycenter-sorted so lines minimally cross.
  */
object StandingQueriesCard {

  val requiredPermissions: Set[String] = Set("StandingQueryRead")

  def apply(standingQueriesSignal: Signal[Pot[Seq[V2StandingQueryInfo]]]): HtmlElement =
    div(child <-- standingQueriesSignal.map(renderState))

  private def renderState(state: Pot[Seq[V2StandingQueryInfo]]): HtmlElement = state match {
    case Pot.Empty =>
      Card(title = "Standing Queries", body = p(cls := "text-muted mb-0", "No standing query data loaded."))

    case Pot.Pending =>
      Card(
        title = "Standing Queries",
        body = div(
          cls := "d-flex align-items-center",
          span(cls := "spinner-border spinner-border-sm me-2"),
          span("Loading standing queries..."),
        ),
      )

    case Pot.Ready(data) =>
      renderContent(data)

    case Pot.Failed(err) =>
      Card(title = "Standing Queries", body = div(cls := "alert alert-danger mb-0", s"Error: $err"))

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

  private def renderContent(queries: Seq[V2StandingQueryInfo]): HtmlElement = {
    if (queries.isEmpty)
      return Card(
        title = "Standing Queries",
        body = p(cls := "text-muted mb-0", "No standing queries configured."),
      )

    Card(
      title = "Standing Queries",
      body = div(
        width := "100%",
        withResizeObserver(queries, renderSankey),
      ),
    )
  }

  /** Render on mount, then re-render whenever the container width changes.
    * `renderFn` is expected to fully reset the container (e.g. via innerHTML = "").
    */
  private def withResizeObserver[A](
    data: A,
    renderFn: (dom.HTMLElement, A) => Unit,
  ): com.raquo.laminar.api.L.Modifier[com.raquo.laminar.nodes.ReactiveHtmlElement[dom.html.Div]] = {
    var observer: js.Dynamic = null
    var rerenderTimeout: Int = 0
    var lastWidth: Int = -1
    com.raquo.laminar.api.L.onMountUnmountCallback(
      mount = ctx => {
        val container = ctx.thisNode.ref
        renderFn(container, data)
        lastWidth = container.clientWidth.toInt
        val ro = js.Dynamic.global.ResizeObserver
        if (!js.isUndefined(ro)) {
          observer = js.Dynamic.newInstance(ro)({ (_: js.Any) =>
            val w = container.clientWidth.toInt
            if (math.abs(w - lastWidth) > 8) {
              if (rerenderTimeout != 0) dom.window.clearTimeout(rerenderTimeout)
              rerenderTimeout = dom.window.setTimeout(
                () => {
                  renderFn(container, data)
                  lastWidth = container.clientWidth.toInt
                },
                120,
              )
            }
          }: js.Function1[js.Any, Unit])
          val _ = observer.observe(container)
        }
      },
      unmount = _ => {
        if (observer != null) observer.disconnect()
        if (rerenderTimeout != 0) dom.window.clearTimeout(rerenderTimeout)
      },
    )
  }

  // Internal display structs
  private case class SQNode(name: String, matchRate: Double, outputs: Seq[OutNode], rawOutputs: Seq[RawOut])
  private case class OutNode(name: String, outputType: String, destKey: String)

  /** One pre-dedup output workflow; the destination tooltip lists these. */
  private case class RawOut(queryName: String, workflowName: String, destinationType: String, queryRate: Double)

  private def renderSankey(container: dom.HTMLElement, queries: Seq[V2StandingQueryInfo]): Unit = {
    // Clear any previous render
    container.innerHTML = ""
    container.style.position = "relative"

    // Convert V2 types → display types.
    // Outputs are grouped on the right by destination TYPE, so all Kafka outputs (regardless
    // of topic or workflow name) collapse to a single "Kafka" node. Two outputs of the same
    // query going to the same type also collapse into a single link rather than stacking
    // duplicated lines between the same pair of nodes.
    val sqs: Seq[SQNode] = queries.map { sq =>
      val matchRate = sq.stats.values.map(_.rates.oneMinute).sum
      val outs = sq.outputs.map { w =>
        val firstDest = w.destinations.headOption
        val outputType = firstDest.map(_.destinationType.toLowerCase).getOrElse("unknown")
        OutNode(w.name, outputType, outputType)
      }
      // Deduplicate outputs that land on the same destination type.
      val distinctOuts = outs.groupBy(_.destKey).map { case (_, group) => group.head }.toSeq
      val raw = sq.outputs.map { w =>
        val outputType = w.destinations.headOption.map(_.destinationType.toLowerCase).getOrElse("unknown")
        RawOut(sq.name, w.name, outputType, matchRate)
      }
      SQNode(sq.name, matchRate, distinctOuts, raw)
    }

    // destKey → full pre-dedup list of (query, workflow, query's match rate) going to that destination type
    val destRawOutputs: Map[String, Seq[RawOut]] =
      sqs.flatMap(_.rawOutputs).groupBy(_.destinationType)

    // Unique destinations (destKey)
    val destTotals: Map[String, Double] = sqs
      .flatMap(sq => sq.outputs.map(o => o.destKey -> sq.matchRate))
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2).sum)
      .toMap
    val destType: Map[String, String] = sqs.flatMap(sq => sq.outputs.map(o => o.destKey -> o.outputType)).toMap

    // Barycenter sort of destinations
    val destBarycenter: Map[String, Double] = destTotals.keys.map { dest =>
      var sumPos, sumWeight = 0.0
      sqs.zipWithIndex.foreach { case (sq, sqI) =>
        if (sq.outputs.exists(_.destKey == dest)) {
          sumPos += sqI * sq.matchRate
          sumWeight += sq.matchRate
        }
      }
      dest -> (if (sumWeight > 0) sumPos / sumWeight else 0.0)
    }.toMap
    val destinations: Seq[String] = destTotals.keys.toSeq.sortBy(destBarycenter)

    // Wrap a label onto multiple lines, preferring breaks on spaces and dashes.
    // `maxCharsPerLine` is a rough budget chosen for the font size at each call site.
    def wrap(s: String, maxCharsPerLine: Int): Seq[String] = {
      if (s.length <= maxCharsPerLine) return Seq(s)
      val lines = scala.collection.mutable.ArrayBuffer[String]()
      var remaining = s
      while (remaining.length > maxCharsPerLine) {
        // Find the latest break point (space or dash) at or before maxCharsPerLine.
        val window = remaining.take(maxCharsPerLine + 1)
        val lastSpace = window.lastIndexOf(' ')
        val lastDash = window.lastIndexOf('-')
        val breakAt = math.max(lastSpace, lastDash)
        if (breakAt <= 0) {
          // No natural break — hard split.
          lines += remaining.take(maxCharsPerLine)
          remaining = remaining.drop(maxCharsPerLine)
        } else {
          // Keep the dash with the preceding fragment; drop the space.
          val takeUpTo = if (remaining.charAt(breakAt) == '-') breakAt + 1 else breakAt
          lines += remaining.take(takeUpTo).trim
          remaining = remaining.drop(takeUpTo).dropWhile(_ == ' ')
        }
      }
      if (remaining.nonEmpty) lines += remaining
      lines.toSeq
    }

    // Geometry
    val width = math.max(container.clientWidth.toInt, 500)
    val rowHeight = 110
    val maxRate = math.max(sqs.map(_.matchRate).maxOption.getOrElse(1.0), 1.0)

    // Query-name block sizing.
    // Wrap budget keeps moderately long names on multiple lines rather than pushing the
    // diagram's left margin out. We size leftX conservatively (generous char width and
    // gap) to make sure right-aligned labels never overflow the card's left edge.
    val queryNameFontSize = 16.0
    val queryNameWrapBudget = 10
    // Rough em-width for 500-weight sans-serif text — bumped up to cover wider glyphs
    // like 'w' and digits. Over-estimation is fine; the left margin just gets a bit more
    // whitespace. Under-estimation causes visible overflow.
    val approxCharWidth = queryNameFontSize * 0.7
    val widestLineChars: Int = sqs
      .flatMap(sq => wrap(sq.name, queryNameWrapBudget).map(_.length))
      .maxOption
      .getOrElse(0)
    val labelBlockWidth = widestLineChars * approxCharWidth
    // Gap between the end of the label text and the colored bar.
    val labelToBarGap = 24.0
    // Small safety margin inside the card before the text starts.
    val leftEdgePadding = 8.0
    val leftX = math.max(leftEdgePadding + labelBlockWidth + labelToBarGap, 100.0)
    // x-coordinate for the text labels' right edge (they use text-anchor: end).
    val labelRightX = leftX - labelToBarGap

    // Room on the right for the icon + multi-line wrapped label.
    val rightMargin = 160.0
    val rightX = width - rightMargin
    val svgHeight = math.max(sqs.size, destinations.size) * rowHeight + 60
    val padding = 50.0
    val contentHeight = svgHeight - padding * 2

    def spreadYs(count: Int): Seq[Double] =
      if (count == 0) Seq.empty
      else if (count == 1) Seq(svgHeight / 2.0)
      else (0 until count).map(i => padding + i * (contentHeight / (count - 1)))

    val sqYs = spreadYs(sqs.size)
    val destYs = spreadYs(destinations.size)
    val destIndex: Map[String, Int] = destinations.zipWithIndex.toMap

    val svg = D3
      .select(container)
      .append("svg")
      .attr("width", width)
      .attr("height", svgHeight)
      .attr("viewBox", s"0 0 $width $svgHeight")

    // Bar height scale: min 4px, max 60% of rowHeight
    val barMax = rowHeight * 0.6
    def barHeight(rate: Double): Double = 4.0 + (rate / maxRate) * (barMax - 4.0)

    val gap = 4.0

    // Sort each query's outputs by destination Y to minimize crossings
    val sortedSqs: Seq[SQNode] = sqs.map { sq =>
      val sorted = sq.outputs.sortBy(o => destIndex.getOrElse(o.destKey, Int.MaxValue))
      sq.copy(outputs = sorted)
    }

    // Per-query effective line thickness (scaled down if stacked outputs exceed the bar)
    val lineThickness: Map[Int, Double] = sortedSqs.zipWithIndex.map { case (sq, sqI) =>
      val base = barHeight(sq.matchRate)
      val n = sq.outputs.size
      val stacked = base * n + gap * (n - 1)
      val scale = if (stacked > base) base / stacked else 1.0
      sqI -> base * scale
    }.toMap

    // Stacked Y-offsets at the query (source) side
    val sqLinkYs: Map[Int, Seq[Double]] = sortedSqs.zipWithIndex.map { case (sq, sqI) =>
      val thick = lineThickness(sqI)
      val n = sq.outputs.size
      val totalH = thick * n + gap * (n - 1)
      var cursor = -totalH / 2
      val centers = sq.outputs.map { _ =>
        val c = cursor + thick / 2
        cursor += thick + gap
        c
      }
      sqI -> centers
    }.toMap

    // Stacked Y-offsets at the destination side
    case class LinkRef(sqI: Int, thickness: Double)
    val destLinks: Map[String, Seq[LinkRef]] = {
      val acc = scala.collection.mutable.Map[String, scala.collection.mutable.ArrayBuffer[LinkRef]]()
      sortedSqs.zipWithIndex.foreach { case (sq, sqI) =>
        sq.outputs.foreach { o =>
          acc.getOrElseUpdate(o.destKey, scala.collection.mutable.ArrayBuffer()) += LinkRef(sqI, lineThickness(sqI))
        }
      }
      acc.view.mapValues(_.toSeq).toMap
    }
    val destLinkYs: Map[String, Seq[Double]] = destLinks.map { case (dest, links) =>
      val totalH = links.map(_.thickness).sum + (links.size - 1) * gap
      var cursor = -totalH / 2
      val centers = links.map { l =>
        val c = cursor + l.thickness / 2
        cursor += l.thickness + gap
        c
      }
      dest -> centers
    }

    val midX = (leftX + rightX) / 2

    // Collect bezier path data for hover overlays (one transparent wider stroke per real path).
    case class LinkOverlay(d: String, sqName: String, destKey: String, rate: Double)
    val linkOverlays = scala.collection.mutable.ArrayBuffer[LinkOverlay]()

    // Draw bezier links: query → destination.
    // Idle queries (rate == 0) draw a thin dark-grey line so the connection is still
    // visible without misleadingly suggesting flow.
    val destLinkIndex = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
    sortedSqs.zipWithIndex.foreach { case (sq, sqI) =>
      val isIdle = sq.matchRate <= 0
      val baseColor = D3.schemeDark2.asInstanceOf[js.Array[String]].apply(sqI % 8)
      val strokeColor = if (isIdle) "#6c757d" else baseColor
      val strokeOpacity = if (isIdle) 0.5 else 0.3
      val thick = if (isIdle) 1.5 else lineThickness(sqI)

      sq.outputs.zipWithIndex.foreach { case (out, outI) =>
        val destI = destIndex(out.destKey)
        val dIdx = destLinkIndex(out.destKey)
        destLinkIndex(out.destKey) = dIdx + 1

        val y1 = sqYs(sqI) + sqLinkYs(sqI)(outI)
        val y2 = destYs(destI) + destLinkYs(out.destKey)(dIdx)

        val d = s"M$leftX,$y1 C$midX,$y1 $midX,$y2 $rightX,$y2"
        svg
          .append("path")
          .attr("d", d)
          .attr("fill", "none")
          .attr("stroke", strokeColor)
          .attr("stroke-width", thick)
          .attr("stroke-opacity", strokeOpacity)

        linkOverlays += LinkOverlay(d, sq.name, out.destKey, sq.matchRate)
      }
    }

    // Transparent wider-stroke overlays on top so hover works even on thin/idle lines.
    linkOverlays.foreach { lo =>
      val path = svg
        .append("path")
        .attr("d", lo.d)
        .attr("fill", "none")
        .attr("stroke", "transparent")
        .attr("stroke-width", 18)
        .attr("pointer-events", "stroke")
        .attr("style", "cursor: pointer;")
      val node = path.node().asInstanceOf[dom.Element]
      LandingTooltip.attachToPath(node, () => buildLinkTooltip(lo.sqName, lo.destKey, lo.rate, destRawOutputs))
    }

    // Draw query labels (left side)
    val barWidth = 6.0
    sortedSqs.zipWithIndex.foreach { case (sq, sqI) =>
      val baseColor = D3.schemeDark2.asInstanceOf[js.Array[String]].apply(sqI % 8)
      val y = sqYs(sqI)
      val barH = barHeight(sq.matchRate)

      // Vertical bar
      svg
        .append("rect")
        .attr("x", leftX - barWidth)
        .attr("y", y - barH / 2)
        .attr("width", barWidth)
        .attr("height", barH)
        .attr("rx", 2)
        .attr("fill", baseColor)

      // Query name
      val nameLines = wrap(sq.name, queryNameWrapBudget)
      val nameFontSize = queryNameFontSize.toInt
      val nameLineHeight = nameFontSize + 2
      val totalNameHeight = nameLines.size * nameLineHeight
      // Center the block of name lines vertically with the bar.
      val nameStartY = y - totalNameHeight / 2.0 + nameFontSize
      nameLines.zipWithIndex.foreach { case (line, idx) =>
        svg
          .append("text")
          .attr("x", labelRightX)
          .attr("y", nameStartY + idx * nameLineHeight)
          .attr("text-anchor", "end")
          .attr("font-size", s"${nameFontSize}px")
          .attr("font-weight", "500")
          .attr("fill", "#0a295b")
          .text(line)
      }

      // Rate below the name block.
      svg
        .append("text")
        .attr("x", labelRightX)
        .attr("y", nameStartY + totalNameHeight + 4)
        .attr("text-anchor", "end")
        .attr("font-size", "13px")
        .attr("fill", "#6c757d")
        .text(formatRate(sq.matchRate))

      // Transparent hover zone covering the name block + bar. Hug the visible content
      // tightly so leaving the row genuinely enters whitespace — an oversized invisible
      // rect would keep the tooltip open over empty space.
      val zoneH = math.max(barH, totalNameHeight.toDouble + nameFontSize) + 12
      val zoneLeft = math.max(leftEdgePadding, labelRightX - labelBlockWidth - 4)
      val zoneRight = leftX + 2
      val zoneRect = svg
        .append("rect")
        .attr("x", zoneLeft)
        .attr("y", y - zoneH / 2)
        .attr("width", math.max(40.0, zoneRight - zoneLeft))
        .attr("height", zoneH)
        .attr("fill", "transparent")
        .attr("pointer-events", "all")
        .attr("style", "cursor: pointer;")
      LandingTooltip.attachToElement(
        zoneRect.node().asInstanceOf[dom.Element],
        () => buildQueryTooltip(sq),
      )
    }

    // Draw destination type icons + labels (right side).
    // One node per destination type — individual output workflow names are not shown
    // (matches the ingests card, which also shows only source types).
    destinations.zipWithIndex.foreach { case (dest, i) =>
      val y = destYs(i)
      val typeSlug = destType.getOrElse(dest, "")
      val iconOpt = ServiceIcons.forType(typeSlug)
      val typeLabel = ServiceIcons.labelFor(typeSlug)
      val iconSize = 40
      val labelAnchorX = rightX + 10 + iconSize / 2

      iconOpt match {
        case Some(iconUri) =>
          svg
            .append("image")
            .attr("href", iconUri)
            .attr("x", rightX + 10)
            .attr("y", y - iconSize / 2)
            .attr("width", iconSize)
            .attr("height", iconSize)
        case None =>
          // No icon: render the type label in big text at the destination slot.
          val fontSize = 18
          svg
            .append("text")
            .attr("x", labelAnchorX)
            .attr("y", y + fontSize / 2.0)
            .attr("text-anchor", "middle")
            .attr("font-size", s"${fontSize}px")
            .attr("font-weight", "600")
            .attr("fill", "#0a295b")
            .text(typeLabel)
      }

      // Transparent hover zone over the destination slot.
      val destZoneH = rowHeight * 0.8
      val destZoneW = math.min(rightMargin, width - rightX)
      val destRect = svg
        .append("rect")
        .attr("x", rightX)
        .attr("y", y - destZoneH / 2)
        .attr("width", destZoneW)
        .attr("height", destZoneH)
        .attr("fill", "transparent")
        .attr("pointer-events", "all")
        .attr("style", "cursor: pointer;")
      LandingTooltip.attachToElement(
        destRect.node().asInstanceOf[dom.Element],
        () => buildDestinationTooltip(dest, typeLabel, destRawOutputs.getOrElse(dest, Nil)),
      )
    }
  }

  // --- Tooltip builders -----------------------------------------------------

  private def buildQueryTooltip(sq: SQNode): String = {
    val kv =
      LandingTooltip.kvRow("Match rate", LandingTooltip.formatRate(sq.matchRate)) +
      LandingTooltip.kvRow("Outputs", s"${sq.rawOutputs.size}") +
      LandingTooltip.kvRow("Destinations", s"${sq.outputs.size}")
    val items = sq.rawOutputs
      .sortBy(-_.queryRate)
      .map(r =>
        LandingTooltip.BreakdownItem(
          name = s"${r.workflowName} → ${ServiceIcons.labelFor(r.destinationType)}",
          detail = LandingTooltip.formatRate(r.queryRate),
        ),
      )
    LandingTooltip.header(sq.name) +
    LandingTooltip.kvTable(kv) +
    LandingTooltip.subheader("Output workflows") +
    LandingTooltip.breakdownTable(items)
  }

  private def buildLinkTooltip(
    queryName: String,
    destKey: String,
    queryRate: Double,
    destRawOutputs: Map[String, Seq[RawOut]],
  ): String = {
    val destLabel = ServiceIcons.labelFor(destKey)
    // Workflows between this specific query and this destination type (pre-dedup).
    val workflows = destRawOutputs.getOrElse(destKey, Nil).filter(_.queryName == queryName)
    val kv =
      LandingTooltip.kvRow("Source query", queryName) +
      LandingTooltip.kvRow("Destination", destLabel) +
      LandingTooltip.kvRow("Rate", LandingTooltip.formatRate(queryRate)) +
      LandingTooltip.kvRow("Workflows", s"${workflows.size}")
    val items = workflows.map(w =>
      LandingTooltip.BreakdownItem(
        name = w.workflowName,
        detail = LandingTooltip.formatRate(w.queryRate),
      ),
    )
    LandingTooltip.header(s"$queryName → $destLabel") +
    LandingTooltip.kvTable(kv) +
    (if (workflows.nonEmpty)
       LandingTooltip.subheader("Workflows on this flow") +
       LandingTooltip.breakdownTable(items)
     else "")
  }

  private def buildDestinationTooltip(
    destKey: String,
    destLabel: String,
    raws: Seq[RawOut],
  ): String = {
    val totalRate = raws.map(_.queryRate).sum
    val distinctQueries = raws.map(_.queryName).distinct.size
    val kv =
      LandingTooltip.kvRow("Destination type", destLabel) +
      LandingTooltip.kvRow("Total rate", LandingTooltip.formatRate(totalRate)) +
      LandingTooltip.kvRow("Queries", s"$distinctQueries") +
      LandingTooltip.kvRow("Workflows", s"${raws.size}")
    val items = raws
      .sortBy(-_.queryRate)
      .map(r =>
        LandingTooltip.BreakdownItem(
          name = s"${r.queryName} → ${r.workflowName}",
          detail = LandingTooltip.formatRate(r.queryRate),
        ),
      )
    LandingTooltip.header(destLabel) +
    LandingTooltip.kvTable(kv) +
    LandingTooltip.subheader("Queries feeding this destination") +
    LandingTooltip.breakdownTable(items)
  }

  private def formatRate(rate: Double): String =
    if (rate < 1) f"$rate%.1f/s"
    else f"${rate.toLong}%,d/s"
}
