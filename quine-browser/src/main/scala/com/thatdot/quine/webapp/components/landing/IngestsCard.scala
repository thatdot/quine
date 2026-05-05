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
  *
  * The Plotly chart is created once on first mount and subsequent ingest-data
  * emissions are pushed through `Plotly.react`, which diffs against the current
  * trace and updates link values smoothly. This avoids the visible flash that
  * comes with `child <--`-driven full re-renders against a 5-second polling cadence.
  */
object IngestsCard {

  val requiredPermissions: Set[String] = Set("IngestRead")

  def apply(ingestsSignal: Signal[Pot[Seq[V2IngestInfo]]]): HtmlElement = {
    // Track observer + debounce timer so we can re-render on container resize.
    var observer: js.Dynamic = null
    var rerenderTimeout: Int = 0
    var lastWidth: Int = -1
    // Plotly is initialized lazily on the first `Ready` emission. Subsequent
    // `Ready` emissions go through `Plotly.react`.
    var plotlyInitialized: Boolean = false
    var lastIngests: Seq[V2IngestInfo] = Nil
    // Structure key for the Sankey — sorted node labels. Overlay/tooltip regeneration
    // is skipped when the key matches, since Plotly preserves DOM identity for the
    // same node set across `react` calls. Skipping the overlay churn eliminates the
    // frame-long flicker that was visible on every poll.
    var lastStructureKey: Seq[String] = Nil
    // Tooltip closures reach back into this cell to render fresh data on hover, so
    // hover content always reflects the latest poll without re-attaching listeners.
    var current: Seq[V2IngestInfo] = Nil

    // Split the source Pot into two narrower signals (data + status) and `.distinct`
    // each so identical polls don't re-fire any side effects. Following the Laminar
    // principle of binding the smallest piece per signal.
    val dataSig: Signal[Option[Seq[V2IngestInfo]]] = ingestsSignal.map(_.toOption).distinct
    val statusSig: Signal[Option[HtmlElement]] = ingestsSignal
      .map[Option[StatusKind]] {
        // PendingStale isn't currently produced by LandingStore; treat like Ready if it ever is.
        // FailedStale: chart keeps showing prior data; the page-level banner in LandingPage
        // surfaces the refresh-failure message across all cards.
        case Pot.Ready(_) | Pot.PendingStale(_) | Pot.FailedStale(_, _) => None
        case Pot.Empty => Some(StatusKind.Empty)
        case Pot.Pending => Some(StatusKind.Pending)
        case Pot.Failed(err) => Some(StatusKind.Failed(err))
      }
      .distinct
      .map(_.map(renderStatus))

    val plotContainer = div(width := "100%")

    def renderInto(container: dom.HTMLElement, ingests: Seq[V2IngestInfo]): Unit = {
      lastIngests = ingests
      current = ingests
      if (ingests.isEmpty) {
        if (plotlyInitialized) {
          PlotlyJS.purge(container)
          plotlyInitialized = false
          lastStructureKey = Nil
        }
        return
      }
      val (traces, layout, config, sourceTypeLabels, structureKey) = buildPlotlyArgs(ingests)
      val structureChanged = structureKey != lastStructureKey
      val wasInitialized = plotlyInitialized
      val promise =
        if (plotlyInitialized) PlotlyJS.react(container, traces, layout, config)
        else PlotlyJS.newPlot(container, traces, layout, config)
      val _ = promise // suppress unused-value warning
      // Plotly rebuilds its SVG `<text>` elements on every `react` call, so the
      // `display: none` we applied to icon-replaced labels gets reset. We re-hide
      // them *synchronously*: by the time `react` returns, Plotly has already written
      // the new `<text>` nodes to the DOM (just not painted them yet), so a synchronous
      // walk catches the labels before any paint. This avoids the split-second flash
      // we'd see if hiding were deferred.
      //
      // The `<img>` overlay elements we appended on the *previous* render are still
      // in place, so we only re-add them when the node set actually changed. That
      // post-processing is deferred via `requestAnimationFrame` which runs after layout
      // but before paint, which is exactly what `addIconOverlays` needs (it calls `getBoundingClientRect()`
      // to position each `<img>` and would get stale measurements if it ran before layout).
      hideIconReplacedLabels(container, sourceTypeLabels)
      if (structureChanged || !wasInitialized) {
        val _ = dom.window.requestAnimationFrame { (_: Double) =>
          addIconOverlays(container, sourceTypeLabels)
          addTooltips(container, () => current)
        }
      }
      lastStructureKey = structureKey
      plotlyInitialized = true
      lastWidth = container.clientWidth.toInt
    }

    Card(
      title = "Ingests on this Host",
      body = div(
        // The Plotly container — its identity stays stable across signal emissions,
        // and Plotly mutates the SVG inside it via `react`.
        plotContainer.amend(
          onMountCallback { ctx =>
            val container = ctx.thisNode.ref
            // If we already saw `Ready` data before mount, draw it now.
            if (lastIngests.nonEmpty) renderInto(container, lastIngests)
            val ro = js.Dynamic.global.ResizeObserver
            if (!js.isUndefined(ro)) {
              observer = js.Dynamic.newInstance(ro)({ (_: js.Any) =>
                val w = container.clientWidth.toInt
                if (math.abs(w - lastWidth) > 8) {
                  if (rerenderTimeout != 0) dom.window.clearTimeout(rerenderTimeout)
                  rerenderTimeout = dom.window.setTimeout(
                    () => {
                      if (plotlyInitialized) {
                        // Force a fresh layout at the new width — `react` alone won't
                        // pick up the container size change reliably.
                        PlotlyJS.purge(container)
                        plotlyInitialized = false
                      }
                      renderInto(container, lastIngests)
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
            plotlyInitialized = false
            if (observer != null) observer.disconnect()
            if (rerenderTimeout != 0) dom.window.clearTimeout(rerenderTimeout)
          },
          // Pump fresh ingest data into Plotly on every emission. `dataSig` is
          // `.distinct`d so a poll that returns the same Seq doesn't even fire here.
          dataSig --> Observer[Option[Seq[V2IngestInfo]]] {
            case Some(data) =>
              val container = plotContainer.ref
              if (container != null) renderInto(container, data)
              else lastIngests = data
            case None => () // status overlay handles the not-loaded case
          },
        ),
        // Status overlay (placeholder/error) sits above the plot container. When the
        // overlay is `None`, only the chart shows. `statusSig` is `.distinct`d on the
        // status *kind*, so the placeholder element is only rebuilt on a real status
        // transition — not on every poll while the card is loaded.
        child.maybe <-- statusSig,
      ),
    )
  }

  /** Status kinds for the placeholder overlay — declared as a separate ADT so the
    * overlay signal can be `.distinct`d on the kind, not on the rendered element.
    */
  sealed private trait StatusKind
  private object StatusKind {
    case object Empty extends StatusKind
    case object Pending extends StatusKind
    final case class Failed(err: String) extends StatusKind
  }

  private def renderStatus(kind: StatusKind): HtmlElement = kind match {
    case StatusKind.Empty => p(cls := "text-muted mb-0", "No ingest data loaded.")
    case StatusKind.Pending =>
      div(
        cls := "d-flex align-items-center",
        span(cls := "spinner-border spinner-border-sm me-2"),
        span("Loading ingests..."),
      )
    case StatusKind.Failed(err) =>
      div(cls := "alert alert-danger mb-0", s"Error: $err")
  }

  /** Compute the Plotly trace, layout, and config arguments plus the source-type-label
    * map needed by the icon-overlay step, and a structure key (sorted node labels)
    * used to decide whether overlays/tooltip zones need re-attaching.
    */
  private def buildPlotlyArgs(
    ingests: Seq[V2IngestInfo],
  ): (
    js.Array[js.Object],
    js.Object,
    js.Object,
    Map[String, String],
    Seq[String],
  ) = {
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

    val structureKey: Seq[String] = nodeLabels.toSeq.sorted
    (
      js.Array(trace.asInstanceOf[js.Object]),
      layout.asInstanceOf[js.Object],
      config.asInstanceOf[js.Object],
      sourceTypeLabels.toMap,
      structureKey,
    )
  }

  /** Extract a common prefix from names like `ingest-1`, `ingest-2` → `ingest`. */
  private val NumberSuffix = """^(.+)-(\d+)$""".r
  private def ingestGroupName(name: String): String = name match {
    case NumberSuffix(prefix, _) => prefix
    case other => other
  }

  /** Re-apply `display: none` to the SVG text labels that are visually replaced by an
    * icon overlay. Plotly's `react` rebuilds those `<text>` elements internally, so
    * each render restores their default visibility — without this re-hiding step the
    * label text reappears alongside the icon on every poll. Cheap and safe to call on
    * every Plotly render, even when the structure key hasn't changed.
    */
  private def hideIconReplacedLabels(container: dom.HTMLElement, sourceTypeLabels: Map[String, String]): Unit = {
    val texts = container.querySelectorAll(".sankey text.node-label")
    if (texts.length == 0) return
    (0 until texts.length).foreach { i =>
      val labelEl = texts.item(i).asInstanceOf[dom.Element]
      val text = Option(labelEl.textContent).map(_.trim).getOrElse("")
      val replaced =
        text == "Quine" ||
        sourceTypeLabels.get(text).flatMap(ServiceIcons.forType).isDefined
      if (replaced) labelEl.asInstanceOf[dom.html.Element].style.display = "none"
    }
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

        // Hide the text label — the icon replaces it. (Also done by
        // `hideIconReplacedLabels` on every Plotly render, since `Plotly.react`
        // rebuilds the `<text>` elements and resets their inline style.)
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

  /** Add hover zones for ingest-group labels, source-type nodes, and the Quine node.
    * Each zone's tooltip is built lazily from `getCurrent()` on hover, so updates to
    * the underlying ingest data are reflected without re-attaching listeners.
    */
  private def addTooltips(
    container: dom.HTMLElement,
    getCurrent: () => Seq[V2IngestInfo],
  ): Unit = {
    val existing = container.querySelectorAll(".ingest-tooltip-zone")
    (0 until existing.length).foreach(i => existing.item(i).asInstanceOf[dom.Element].remove())

    val texts = container.querySelectorAll(".sankey text.node-label")
    if (texts.length == 0) return

    val containerRect = container.getBoundingClientRect()

    def placeZone(labelEl: dom.Element, buildHtml: () => String, extraPadX: Int = 8, extraPadY: Int = 8): Unit = {
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
        (_: dom.Event) => LandingTooltip.showNear(buildHtml(), zone.getBoundingClientRect()),
      )
      zone.addEventListener("mouseleave", (_: dom.Event) => LandingTooltip.hide())
      val _ = container.appendChild(zone)
    }

    (0 until texts.length).foreach { i =>
      val labelEl = texts.item(i).asInstanceOf[dom.Element]
      val text = Option(labelEl.textContent).map(_.trim).getOrElse("")

      // Resolve which kind of node this is by checking the current ingest data. The
      // structure key guarantees these lookups will continue to work for the lifetime
      // of these listeners (regenerated when the structure changes).
      def groupMembersFor(label: String, current: Seq[V2IngestInfo]): Option[Seq[V2IngestInfo]] = {
        val candidates = current
          .groupBy(ig => (ig.sourceId, ingestGroupName(ig.name)))
          .map { case ((_, prefix), members) =>
            val l = if (members.size > 1) s"$prefix (×${members.size})" else prefix
            l -> members
          }
        candidates.get(label)
      }
      def sourceMembersFor(label: String, current: Seq[V2IngestInfo]): Option[(String, Seq[V2IngestInfo])] =
        current.groupBy(_.sourceId).get(label).map { members =>
          (members.head.sourceType.toLowerCase, members)
        }

      // Decide tooltip type once at attach time based on the current data, but rebuild
      // the contents from `getCurrent()` on each hover.
      val current0 = getCurrent()
      if (text == "Quine") {
        placeZone(labelEl, () => buildQuineTooltip(getCurrent()), extraPadX = 12, extraPadY = 12)
      } else if (groupMembersFor(text, current0).isDefined) {
        placeZone(
          labelEl,
          () => buildGroupTooltip(text, groupMembersFor(text, getCurrent()).getOrElse(Nil)),
        )
      } else if (sourceMembersFor(text, current0).isDefined) {
        placeZone(
          labelEl,
          () => {
            val (st, members) = sourceMembersFor(text, getCurrent()).getOrElse(("", Nil))
            buildSourceTooltip(text, st, members)
          },
        )
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
