package com.thatdot.quine.webapp.components.landing

import scala.scalajs.js
import scala.scalajs.js.JSConverters._

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.webapp.components.dashboard.Card
import com.thatdot.quine.webapp.components.landing.V2ApiTypes.V2IngestInfo
import com.thatdot.quine.webapp.components.{D3, PlotlyJS}
import com.thatdot.quine.webapp.util.Pot

/** Card displaying ingest streams as a Plotly Sankey:
  *   source type (kafka, s3, ...) → ingest name group → Quine
  *
  * Link thickness is proportional to the ingest's one-minute rate.
  * Ingests whose names share a common `name-N` suffix are merged into a
  * single "name (×N)" group node, matching the prototype's behavior.
  */
object IngestsCard {

  val requiredPermissions: Set[String] = Set("IngestRead")

  def apply(ingestsSignal: Signal[Pot[Seq[V2IngestInfo]]]): HtmlElement =
    div(child <-- ingestsSignal.map(renderState))

  private def renderState(state: Pot[Seq[V2IngestInfo]]): HtmlElement = state match {
    case Pot.Empty =>
      Card(title = "Ingests", body = p(cls := "text-muted mb-0", "No ingest data loaded."))

    case Pot.Pending =>
      Card(
        title = "Ingests",
        body = div(
          cls := "d-flex align-items-center",
          span(cls := "spinner-border spinner-border-sm me-2"),
          span("Loading ingests..."),
        ),
      )

    case Pot.Ready(data) =>
      renderContent(data)

    case Pot.Failed(err) =>
      Card(title = "Ingests", body = div(cls := "alert alert-danger mb-0", s"Error: $err"))

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

  private def renderContent(ingests: Seq[V2IngestInfo]): HtmlElement = {
    if (ingests.isEmpty)
      return Card(title = "Ingests", body = p(cls := "text-muted mb-0", "No ingests configured."))

    // Track observer + debounce timer so we can re-render on container resize.
    var observer: js.Dynamic = null
    var rerenderTimeout: Int = 0
    var lastWidth: Int = -1

    Card(
      title = "Ingests",
      body = div(
        width := "100%",
        onMountCallback { ctx =>
          val container = ctx.thisNode.ref
          renderSankey(container, ingests)
          lastWidth = container.clientWidth.toInt
          val ro = js.Dynamic.global.ResizeObserver
          if (!js.isUndefined(ro)) {
            observer = js.Dynamic.newInstance(ro)({ (_: js.Any) =>
              val w = container.clientWidth.toInt
              if (math.abs(w - lastWidth) > 8) {
                if (rerenderTimeout != 0) dom.window.clearTimeout(rerenderTimeout)
                rerenderTimeout = dom.window.setTimeout(
                  () => {
                    PlotlyJS.purge(container)
                    renderSankey(container, ingests)
                    lastWidth = container.clientWidth.toInt
                  },
                  120,
                )
              }
            }: js.Function1[js.Any, Unit])
            val _ = observer.observe(container)
          }
        },
        onUnmountCallback { el =>
          PlotlyJS.purge(el.ref)
          if (observer != null) observer.disconnect()
          if (rerenderTimeout != 0) dom.window.clearTimeout(rerenderTimeout)
        },
      ),
    )
  }

  /** Extract a common prefix from names like `ingest-1`, `ingest-2` → `ingest`. */
  private val NumberSuffix = """^(.+)-(\d+)$""".r
  private def ingestGroupName(name: String): String = name match {
    case NumberSuffix(prefix, _) => prefix
    case other => other
  }

  private def renderSankey(container: dom.HTMLElement, ingests: Seq[V2IngestInfo]): Unit = {
    // Three-column Sankey: source → ingest group → Quine.
    // Group first by source identifier (e.g. Kafka bootstrap servers, S3 bucket), then
    // within each source by name prefix (so `ingest-1` and `ingest-2` collapse to "ingest (×2)").
    val bySourceId: Seq[(String, Seq[V2IngestInfo])] =
      ingests.groupBy(_.sourceId).toSeq.sortBy(_._1)

    // Build Sankey arrays.
    val nodeLabels = scala.collection.mutable.ArrayBuffer[String]()
    val nodeColors = scala.collection.mutable.ArrayBuffer[String]()
    val nodeIndex = scala.collection.mutable.Map[String, Int]()

    def getNodeIdx(key: String, label: String, color: String): Int =
      nodeIndex.getOrElseUpdate(
        key, {
          nodeLabels += label
          nodeColors += color
          nodeLabels.size - 1
        },
      )

    val linkSources = scala.collection.mutable.ArrayBuffer[Int]()
    val linkTargets = scala.collection.mutable.ArrayBuffer[Int]()
    val linkValues = scala.collection.mutable.ArrayBuffer[Double]()
    val linkColors = scala.collection.mutable.ArrayBuffer[String]()

    val quineIdx = getNodeIdx("__quine__", "Quine", "#0a295b")

    // source-node text → type slug (so icon overlay can resolve)
    val sourceTypeLabels = scala.collection.mutable.Map[String, String]()
    // ingest-group label → member ingests (for tooltips)
    val groupTooltipData = scala.collection.mutable.Map[String, Seq[V2IngestInfo]]()
    // source-node label → (sourceType, member ingests) for tooltips
    val sourceTooltipData = scala.collection.mutable.Map[String, (String, Seq[V2IngestInfo])]()

    // Rates often span orders of magnitude (e.g. 1/s vs 1000/s), which makes the
    // smallest flows invisible when link heights are linearly proportional. We compress
    // the ratio via sqrt so a 1000× true ratio becomes ~32× visual, while preserving
    // ordering. Tooltips still show true rates.
    def compress(rate: Double): Double = math.sqrt(math.max(rate, 0.0))

    bySourceId.zipWithIndex.foreach { case ((sourceId, sourceIngests), srcI) =>
      val baseColor = D3.schemeTableau10.asInstanceOf[js.Array[String]].apply(srcI % 10)
      val sourceType = sourceIngests.head.sourceType.toLowerCase

      // Left column: one node per source identifier, labeled with the source's text.
      val sourceLabel = sourceId
      val sourceIdx = getNodeIdx(s"__source_$sourceId", sourceLabel, baseColor)
      sourceTypeLabels(sourceLabel) = sourceType
      sourceTooltipData(sourceLabel) = (sourceType, sourceIngests)

      // Middle column: group ingests by name prefix within this source.
      val byPrefix: Seq[(String, Seq[V2IngestInfo])] =
        sourceIngests.groupBy(ig => ingestGroupName(ig.name)).toSeq.sortBy(_._1)

      byPrefix.foreach { case (prefix, members) =>
        val label =
          if (members.size > 1) s"$prefix (×${members.size})" else prefix

        // Ingest-group node and link both use the source's identity color so flows
        // are easy to trace by lane. Status is conveyed via the tooltip only — inside
        // a Sankey, repurposing node/label colors for state competes with the
        // diagram's own encoding of flow identity.
        val lighter = D3.hsl(D3.color(baseColor)).brighter(0.5).asInstanceOf[js.Dynamic]
        val lighterColor = lighter.toString().asInstanceOf[String]

        val igIdx = getNodeIdx(s"__ig_${sourceId}_$prefix", label, lighterColor)
        groupTooltipData(label) = members

        val totalRate = members.map(_.stats.rates.oneMinute).sum
        val value = math.max(compress(totalRate), 0.5)

        // Source → ingest-group link
        val srcLinkColor = D3.color(baseColor).asInstanceOf[js.Dynamic]
        srcLinkColor.opacity = 0.2
        linkSources += sourceIdx
        linkTargets += igIdx
        linkValues += value
        linkColors += srcLinkColor.toString().asInstanceOf[String]

        // Ingest-group → Quine link (lighter shade of the same source color)
        val igLinkColor = D3.color(lighterColor).asInstanceOf[js.Dynamic]
        igLinkColor.opacity = 0.2
        linkSources += igIdx
        linkTargets += quineIdx
        linkValues += value
        linkColors += igLinkColor.toString().asInstanceOf[String]
      }
    }

    val sankeyNode = js.Dynamic.literal(
      pad = 20,
      // Thick enough that the right-edge "Quine" bar can host the Quine logo without
      // the logo overflowing the bar on either side.
      thickness = 44,
      line = js.Dynamic.literal(color = "rgba(10, 41, 91, 0.3)", width = 1),
      label = nodeLabels.toJSArray,
      color = nodeColors.toJSArray,
    )

    val sankeyLink = js.Dynamic.literal(
      source = linkSources.toJSArray,
      target = linkTargets.toJSArray,
      value = linkValues.toJSArray,
      color = linkColors.toJSArray,
    )

    val trace = js.Dynamic.literal(
      `type` = "sankey",
      orientation = "h",
      node = sankeyNode,
      link = sankeyLink,
    )

    val layout = js.Dynamic.literal(
      font = js.Dynamic.literal(size = 14, family = "sans-serif", color = "#0a295b"),
      margin = js.Dynamic.literal(t = 14, l = 14, r = 14, b = 14),
      paper_bgcolor = "transparent",
      plot_bgcolor = "transparent",
      height = math.max(ingests.size * 45 + 60, 300),
    )

    val config = js.Dynamic.literal(
      displayModeBar = false,
      responsive = true,
      staticPlot = true,
    )

    PlotlyJS.newPlot(
      container,
      js.Array(trace.asInstanceOf[js.Object]),
      layout.asInstanceOf[js.Object],
      config.asInstanceOf[js.Object],
    )
    // Defer overlay rendering until Plotly has committed the SVG to the DOM.
    val _ = dom.window.setTimeout(
      () => {
        addIconOverlays(container, sourceTypeLabels.toMap)
        addTooltips(container, groupTooltipData.toMap, sourceTooltipData.toMap, ingests)
      },
      0,
    )
  }

  /** For source-type nodes that have a known icon, hide the text label and overlay the icon
    * just to the right of the node bar. Other node labels (ingest-group nodes, Quine) keep
    * their text labels.
    */
  private def addIconOverlays(container: dom.HTMLElement, sourceTypeLabels: Map[String, String]): Unit = {
    val existing = container.querySelectorAll(".ingest-icon-overlay")
    (0 until existing.length).foreach(i => existing.item(i).asInstanceOf[dom.Element].remove())

    val texts = container.querySelectorAll(".sankey text.node-label")
    if (texts.length == 0) return

    // Parent must be positioned so absolute children anchor to it.
    if (container.style.position.isEmpty) container.style.position = "relative"

    val containerRect = container.getBoundingClientRect()
    val iconSize = 26
    val quineIconSize = 40

    (0 until texts.length).foreach { i =>
      val labelEl = texts.item(i).asInstanceOf[dom.Element]
      val text = Option(labelEl.textContent).map(_.trim).getOrElse("")

      // Look up the node-rect (bar) that this label belongs to.
      val rectOpt: Option[dom.Element] = Option(labelEl.parentNode)
        .flatMap(parent => Option(parent.asInstanceOf[dom.Element].querySelector("rect.node-rect")))

      def overlay(iconUri: String, size: Int, placement: String): Unit = {
        val rect = rectOpt.getOrElse(labelEl)
        val rectBox = rect.getBoundingClientRect()

        // Hide the text label — the icon replaces it.
        labelEl.asInstanceOf[dom.html.Element].style.display = "none"

        val (leftPx, topPx) = placement match {
          case "right" =>
            (rectBox.right - containerRect.left + 6, rectBox.top - containerRect.top + (rectBox.height - size) / 2)
          case "center" =>
            (
              rectBox.left - containerRect.left + (rectBox.width - size) / 2,
              rectBox.top - containerRect.top + (rectBox.height - size) / 2,
            )
          case _ =>
            (
              rectBox.left - containerRect.left - size - 6,
              rectBox.top - containerRect.top + (rectBox.height - size) / 2,
            )
        }

        val img = dom.document.createElement("img").asInstanceOf[dom.html.Image]
        img.src = iconUri
        img.className = "ingest-icon-overlay"
        img.setAttribute(
          "style",
          s"position: absolute; max-width: ${size}px; max-height: ${size}px; " +
          s"width: auto; height: auto; object-fit: contain; pointer-events: none; " +
          s"top: ${topPx}px; left: ${leftPx}px;",
        )
        val _ = container.appendChild(img)
      }

      if (text == "Quine") {
        // Quine (white logo) is painted on top of the dark-blue node bar.
        overlay(ServiceIcons.quineIcon, quineIconSize, "center")
      } else {
        sourceTypeLabels.get(text).flatMap(ServiceIcons.forType).foreach { iconUri =>
          // Source-type icons sit just to the right of each source node bar.
          overlay(iconUri, iconSize, "right")
        }
      }
    }
  }

  /** Add hover zones for ingest-group labels, source-type nodes, and the Quine node. */
  private def addTooltips(
    container: dom.HTMLElement,
    groupTooltipData: Map[String, Seq[V2IngestInfo]],
    sourceTooltipData: Map[String, (String, Seq[V2IngestInfo])],
    allIngests: Seq[V2IngestInfo],
  ): Unit = {
    val existing = container.querySelectorAll(".ingest-tooltip-zone")
    (0 until existing.length).foreach(i => existing.item(i).asInstanceOf[dom.Element].remove())

    val texts = container.querySelectorAll(".sankey text.node-label")
    if (texts.length == 0) return

    val containerRect = container.getBoundingClientRect()

    def placeZone(labelEl: dom.Element, html: String, extraPadX: Int = 8, extraPadY: Int = 8): Unit = {
      val rawLabelRect = labelEl.getBoundingClientRect()
      // A hidden text label (Quine / source-type nodes whose labels are replaced by
      // an icon overlay) reports a zero-size rect at (0,0). Treat that as "no label"
      // so the union below doesn't stretch the zone to the viewport origin.
      val labelRectOpt: Option[dom.DOMRect] =
        if (rawLabelRect.width <= 0 || rawLabelRect.height <= 0) None
        else Some(rawLabelRect)
      // The ingest-group's colored bar lives as a sibling `rect.node-rect` within the same
      // Plotly node group. Extend the hover zone to cover both the bar and the text.
      val barRectOpt: Option[dom.DOMRect] = Option(labelEl.parentNode)
        .flatMap(p => Option(p.asInstanceOf[dom.Element].querySelector("rect.node-rect")))
        .map(_.getBoundingClientRect())
      // If neither label nor bar is measurable, bail — we have no geometry to anchor on.
      if (labelRectOpt.isEmpty && barRectOpt.isEmpty) return
      val rects = labelRectOpt.toSeq ++ barRectOpt.toSeq
      val top = rects.map(_.top).min
      val bottom = rects.map(_.bottom).max
      val left = rects.map(_.left).min
      val right = rects.map(_.right).max

      val zone = dom.document.createElement("div").asInstanceOf[dom.html.Div]
      zone.className = "ingest-tooltip-zone"
      zone.setAttribute(
        "style",
        s"position: absolute; cursor: pointer; " +
        s"top: ${top - containerRect.top - extraPadY}px; " +
        s"left: ${left - containerRect.left - extraPadX}px; " +
        s"width: ${right - left + 2 * extraPadX}px; " +
        s"height: ${bottom - top + 2 * extraPadY}px;",
      )
      zone.addEventListener(
        "mouseenter",
        (_: dom.Event) => LandingTooltip.showNear(html, zone.getBoundingClientRect()),
      )
      zone.addEventListener("mouseleave", (_: dom.Event) => LandingTooltip.hide())
      val _ = container.appendChild(zone)
    }

    (0 until texts.length).foreach { i =>
      val labelEl = texts.item(i).asInstanceOf[dom.Element]
      val text = Option(labelEl.textContent).map(_.trim).getOrElse("")

      groupTooltipData.get(text).foreach { members =>
        placeZone(labelEl, buildGroupTooltip(text, members))
      }

      sourceTooltipData.get(text).foreach { case (sourceType, members) =>
        placeZone(labelEl, buildSourceTooltip(text, sourceType, members))
      }

      if (text == "Quine") {
        placeZone(labelEl, buildQuineTooltip(allIngests), extraPadX = 12, extraPadY = 12)
      }
    }
  }

  private def buildGroupTooltip(title: String, members: Seq[V2IngestInfo]): String =
    members match {
      case Seq(only) =>
        // Single-ingest case: the breakdown table would just repeat the totals, so
        // present each attribute as its own labeled row instead.
        LandingTooltip.header(title) +
          LandingTooltip.kvTable(
            LandingTooltip.kvRow("Name", only.name) +
            LandingTooltip.kvRow("Status", only.status, Some(LandingTooltip.statusColor(only.status))) +
            LandingTooltip.kvRow("Rate", LandingTooltip.formatRate(only.stats.rates.oneMinute)) +
            LandingTooltip.kvRow("Count", LandingTooltip.formatCount(only.stats.ingestedCount)),
          )
      case _ =>
        val totalRate = members.map(_.stats.rates.oneMinute).sum
        val totalCount = members.map(_.stats.ingestedCount).sum
        val sorted = members.sortBy(-_.stats.rates.oneMinute)
        val items = sorted.map(m =>
          LandingTooltip.BreakdownItem(m.name, m.status, LandingTooltip.formatRate(m.stats.rates.oneMinute)),
        )
        LandingTooltip.header(title) +
        LandingTooltip.kvTable(
          LandingTooltip.kvRow("Total rate", LandingTooltip.formatRate(totalRate)) +
          LandingTooltip.kvRow("Total count", LandingTooltip.formatCount(totalCount)) +
          LandingTooltip.kvRow("Ingests", s"${members.size}"),
        ) +
        LandingTooltip.subheader("Individual ingests") +
        LandingTooltip.breakdownTable(items)
    }

  private def buildSourceTooltip(title: String, sourceType: String, members: Seq[V2IngestInfo]): String = {
    val totalRate = members.map(_.stats.rates.oneMinute).sum
    val distinctIds = members.map(_.sourceId).distinct
    val kv =
      LandingTooltip.kvRow("Source type", ServiceIcons.labelFor(sourceType.toLowerCase)) +
      LandingTooltip.kvRow("Total rate", LandingTooltip.formatRate(totalRate)) +
      LandingTooltip.kvRow("Streams", s"${members.size}") +
      LandingTooltip.kvRow("Endpoints", s"${distinctIds.size}")

    val sorted = members.sortBy(-_.stats.rates.oneMinute)
    val items = sorted.map(m =>
      LandingTooltip.BreakdownItem(m.name, m.status, LandingTooltip.formatRate(m.stats.rates.oneMinute)),
    )

    LandingTooltip.header(title) +
    LandingTooltip.kvTable(kv) +
    LandingTooltip.subheader("Streams from this source") +
    LandingTooltip.breakdownTable(items)
  }

  private def buildQuineTooltip(allIngests: Seq[V2IngestInfo]): String = {
    val totalRate = allIngests.map(_.stats.rates.oneMinute).sum
    val totalCount = allIngests.map(_.stats.ingestedCount).sum
    val sourceTypes = allIngests.map(_.sourceType).distinct
    val running = allIngests.count(_.status == "Running")
    val paused = allIngests.count(i => i.status == "Paused" || i.status == "Restored")
    val failed = allIngests.count(_.status == "Failed")

    val kv =
      LandingTooltip.kvRow("Total ingest rate", LandingTooltip.formatRate(totalRate)) +
      LandingTooltip.kvRow("Total ingested", LandingTooltip.formatCount(totalCount)) +
      LandingTooltip.kvRow("Streams", s"${allIngests.size}") +
      LandingTooltip.kvRow("Source types", s"${sourceTypes.size}") +
      LandingTooltip.kvRow("Running", s"$running", Some(LandingTooltip.statusColor("Running"))) +
      (if (paused > 0) LandingTooltip.kvRow("Paused/Restored", s"$paused", Some(LandingTooltip.statusColor("Paused")))
       else "") +
      (if (failed > 0) LandingTooltip.kvRow("Failed", s"$failed", Some(LandingTooltip.statusColor("Failed"))) else "")

    LandingTooltip.header("Quine") +
    LandingTooltip.kvTable(kv)
  }

}
