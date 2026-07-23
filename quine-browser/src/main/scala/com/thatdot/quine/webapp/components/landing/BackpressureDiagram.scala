package com.thatdot.quine.webapp.components.landing

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.webapp.components.D3
import com.thatdot.quine.webapp.dataservice.{
  Aggregated,
  BackpressureService,
  BackpressureView,
  HostRef,
  IngestView,
  MemberStatus,
  PressureLevel,
  QueueView,
  SqOutputView,
  StandingQueryView,
}
import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes._

/** High-fidelity backpressure & throughput diagram.
  *
  * Layout: left-to-right flow through the Quine pipeline:
  *   [Ingest cards] → [Valve bar] → [Write tabs + Q node] → [SQ queue meters] → [Output cards w/ enrichment]
  *   + Persistor card below Q + Cluster card above Q if relevant
  *
  * Renders as a responsive SVG
  */
object BackpressureDiagram {

  // ── Design tokens ──
  private object C {
    val ink = "#0a295b"
    val brite = "#1658b7"
    val briteLite = "#4a82d8"
    val gray = "#9aa0b8"
    val muted = "#6c7390"
    val track = "#e7e9f2"
    val green = "#12a572" // flowing
    val restored = "#7fa892" // gray-green: restored (loaded from a prior session, not yet started)
    val yellow = "#e0c020" // constrained
    val amber = "#d98a2b" // backpressured
    val red = "#e0384d" // stopped
    val white = "#ffffff"
  }

  private def stateColor(level: PressureLevel): String = level match {
    case PressureLevel.Flowing => C.green
    case PressureLevel.Constrained => C.yellow
    case PressureLevel.Backpressured => C.amber
  }

  private def levelLabel(level: PressureLevel): String = level match {
    case PressureLevel.Flowing => "FLOWING"
    case PressureLevel.Constrained => "CONSTRAINED"
    case PressureLevel.Backpressured => "BACKPRESSURED"
  }

  /** Tint for a member's chip in the picker. A member that has stopped reporting is red rather than
    * any shade of pressure: we do not know that it is backpressured, we know we cannot see it, and
    * those warrant different reactions from whoever is looking at the page.
    */
  private def memberChipColor(status: MemberStatus): String =
    if (!status.isReporting) C.red else stateColor(status.worst)

  /** A member worth jumping to: under pressure, or not answering at all. */
  private def memberNeedsAttention(status: MemberStatus): Boolean =
    !status.isReporting || status.worst != PressureLevel.Flowing

  /** Name the cluster members responsible for a point's state, for a tooltip.
    *
    * A cluster's whole difficulty is that "backpressured" is not a property of the system but of
    * particular members: the user needs to know *which* ones, not merely that some member is at
    * fault. Empty on OSS (no members), where the caller should simply say nothing.
    */
  private def memberList(members: Seq[Int]): String = members match {
    case Seq() => ""
    case Seq(one) => s"member $one"
    case many => s"members ${many.init.mkString(", ")} and ${many.last}"
  }

  /** One tooltip row for a point that was reduced across cluster members: its state now, and the
    * members responsible for it.
    *
    * This is the payoff of keeping attribution through the reduction ([[Aggregated]]). The color of a
    * point is the worst member's, and without this the user would be told the system is backpressured
    * while being given no way to find out where — the aggregate having thrown away the only fact that
    * would let them act. On OSS there are no members, `perHost` has one anonymous entry, and this
    * degrades to exactly the row it would have been anyway.
    */
  private def levelRow(label: String, agg: Aggregated[PressureLevel]): String = {
    val level = agg.combined
    tipRow(label, levelLabel(level), Some(stateColor(level))) +
    memberBreakdown(agg.perHost)
  }

  /** The same, for a standing query's queue — whose level is a depth against a threshold rather than
    * a gauge, so it is judged by [[QueueView.level]] rather than read off a state.
    */
  private def queueRow(label: String, agg: Aggregated[QueueView]): String = {
    val level = agg.combined.level
    tipRow(label, levelLabel(level), Some(stateColor(level))) +
    memberBreakdown(agg.perHost.map { case (h, q) => (h, q.level) })
  }

  /** A grouped per-member breakdown for one aggregated point: one indented row per distinct level
    * present across the members, each with a color swatch, the level name, and the members at that
    * level, worst level first.
    *
    * Rendered only when the members actually disagree. When they are all at one level the combined
    * row above already said everything — and repeating "all members backpressured" as its own broken-
    * out list, on every tooltip, is noise. So this appears exactly when it carries information the
    * combined value cannot: that the point is worse on some members than others. Members without an
    * index (OSS) never reach here, since there is nothing to attribute across one host.
    */
  private def memberBreakdown(perHost: Seq[(HostRef, PressureLevel)]): String = {
    val byLevel: Map[PressureLevel, Seq[Int]] = perHost
      .collect { case (h, lvl) if h.memberIdx.isDefined => (lvl, h.memberIdx.get) }
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2).sorted)
      .toMap
    if (byLevel.sizeIs < 2) ""
    else {
      val worstFirst = Seq(PressureLevel.Backpressured, PressureLevel.Constrained, PressureLevel.Flowing)
      worstFirst
        .filter(byLevel.contains)
        .zipWithIndex
        .map { case (lvl, idx) =>
          val col = stateColor(lvl)
          // A hairline rule sets the whole breakdown off from the combined row above it.
          val topRule =
            if (idx == 0) "border-top:1px solid rgba(255,255,255,0.12);padding-top:5px;margin-top:2px;" else ""
          s"""<tr><td colspan="2" style="padding:2px 0 2px 12px;line-height:1.45;$topRule">""" +
          s"""<span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:$col;""" +
          """margin-right:7px;vertical-align:baseline;"></span>""" +
          s"""<span style="color:$col;font-weight:600;font-size:12.5px;">${LandingTooltip.escape(
            levelLabel(lvl),
          )}</span>""" +
          s"""<span style="color:#8b93b0;font-size:12.5px;"> · ${LandingTooltip.escape(
            memberList(byLevel(lvl)),
          )}</span>""" +
          "</td></tr>"
        }
        .mkString
    }
  }

  /** Which member(s) a collapsed point runs on. An ingest with the same name may run on more than one
    * member; naming them is how the user finds the stream they are looking at. Empty on OSS, which
    * has no members.
    */
  private def membersRow(members: Seq[Int]): String = members match {
    case Seq() => ""
    case Seq(one) => tipRow("Member", one.toString)
    case many => tipRow("Members", many.mkString(", "))
  }

  /** Which named graph a thing belongs to. Always shown, and not only when several graphs are in
    * view: a name is ambiguous across graphs whether or not the others happen to be filtered out at
    * this moment, and a tooltip that quietly changes what it discloses depending on a filter is a
    * worse deal than one extra row.
    */
  private def graphRow(namespace: String): String = tipRow("Graph", namespace)

  /** Tint for an ingest that is not actively running (shown as not flowing). `None` means running —
    * the card should use its live per-stage colors instead.
    */
  private def ingestStatusTint(status: String): Option[String] =
    if (status == "RUNNING") None
    else if (status == "FAILED") Some(C.red)
    else if (status == "RESTORED") Some(C.restored)
    else Some(C.gray) // PAUSED / Completed / Terminated — loaded but not flowing

  // ── Layout constants (from prototype) ──
  private val W = 1320.0
  private val RowGap = 88.0

  /** Minimum vertical gap between two standing queries' result-queue meters. A meter carries its
    * query's name above it and its fill percentage below, so meters closer than this overlap their
    * own labels.
    */
  private val SqMinGap = 114.0
  private val BoxW = 234.0
  private val Edge = 20.0
  private val QmW = 26.0 // queue meter width
  private val ValveX = 400.0
  private val QueueX = W - ValveX - QmW // ≈894, mirrored

  // ── Cluster info for diagram rendering ──
  /** What the diagram knows about the cluster's identity.
    *
    * `hotSpareCount` and `fullyUp` are optional because they are only knowable from the cluster
    * status endpoint, which a user may not have permission to read and which has not answered yet
    * during the first moments after load. The member count, by contrast, is always available — the
    * backpressure snapshots themselves report which members answered.
    *
    * When they are unknown the diagram omits those lines rather than inventing them. Defaulting them
    * (to "0 hot spares" and "Operational", as the version this was ported from did) would state as
    * fact two things we did not measure, and the second is exactly the thing someone reading this
    * page is trying to find out.
    */
  private case class ClusterInfo(memberCount: Int, hotSpareCount: Option[Int], fullyUp: Option[Boolean]) {
    def isDegraded: Boolean = fullyUp.contains(false)
  }

  // ── Identity ──

  /** Identity of a thing in the diagram: what the bottleneck names, what the sticky enrichment cache
    * keys on, and what one standing query's ribbons match against to find their own outputs.
    *
    * Namespaced, because a name is not an identity. Two graphs may each have an ingest called
    * `orders` or a standing query called `high-value`; the store keeps them apart (it groups on
    * `(namespace, name)`), and the diagram must too. Keyed on the bare name, one graph's bottleneck
    * lights the other's ribbon and lands its pill on the wrong card, two same-named queries' meters
    * are assigned the same row and draw on top of each other, and the enrichment cache — which is
    * sticky, and outlives any one render — marks an output as enriched because an unrelated output
    * of the same name in another graph was.
    *
    * The unit separator cannot occur in a user-facing name, so these keys cannot collide by
    * construction.
    */
  private val KeySep: String = 31.toChar.toString // ASCII unit separator (0x1F)

  private def idKey(parts: String*): String = parts.mkString(KeySep)

  private def ingestKey(p: IngestView): String = idKey(p.namespace, p.name)
  private def sqKey(sq: StandingQueryView): String = idKey(sq.namespace, sq.name)
  private def outputKey(sq: StandingQueryView, o: SqOutputView): String =
    idKey(sq.namespace, sq.name, o.name)

  // ── Derived state ──
  /** A point in the pipeline that could be the bottleneck.
    *
    * @param state worst state across the members in scope — what colors the point
    */
  private case class BpSignal(
    signalType: String,
    id: String,
    label: String,
    state: PressureLevel,
    rank: Int,
  )

  /** Compute the bottleneck:
    * - Collect all signals with their downstream rank:
    *   SQ output destinations (rank 6) > enrichment (5) > SQ queue (4) > graph-write/ack (3)
    * - Filter to only backpressured signals (CONSTRAINED is informational, never a bottleneck)
    * - The highest-rank backpressured signal is the bottleneck
    * - When valve is closed, ingest stages are excluded (the cause is downstream)
    */
  private def computeBottleneck(view: BackpressureView): Option[BpSignal] = {
    val signals = scala.collection.mutable.ArrayBuffer[BpSignal]()
    // SQ output destinations: rank 6 (most downstream — the actual slow sink)
    view.standingQueries.foreach { sq =>
      sq.outputs.foreach { o =>
        val outKey = outputKey(sq, o)
        o.destinations.foreach { d =>
          signals += BpSignal("sq-output", outKey, s"Output: ${o.name}", d.state.combined, 6)
        }
        // SQ output enrichment: rank 5
        o.enrichment.foreach { e =>
          signals += BpSignal("sq-enrichment", outKey, s"Output: ${o.name}", e.combined, 5)
        }
      }
      // SQ queues: rank 4
      signals += BpSignal("sq", sqKey(sq), s"Standing Query: ${sq.name}", sq.queue.combined.level, 4)
    }
    // Ingest stages (only when valve is open — when closed, the cause is downstream).
    // Only actively-running ingests can be a bottleneck: paused/restored/failed/terminal ingests
    // aren't processing, and their gauges are stale or absent.
    if (view.globalValve.combined.isOpen) {
      view.ingests.filter(_.isRunning).foreach { p =>
        val pKey = ingestKey(p)
        signals += BpSignal("gw", pKey, s"Graph: ${p.name}", p.preGraphWrite.combined, 3)
        p.postGraphWrite.foreach { s =>
          signals += BpSignal("ack", pKey, s"Graph: ${p.name}", s.combined, 3)
        }
      }
    }
    // Only BACKPRESSURED counts as a bottleneck; CONSTRAINED is informational only
    signals.filter(_.state == PressureLevel.Backpressured).sortBy(-_.rank).headOption
  }

  private def systemStatus(view: BackpressureView, bottleneck: Option[BpSignal]): (String, String) =
    if (!view.globalValve.combined.isOpen) ("Stopped", C.red)
    else if (bottleneck.isDefined) ("Backpressured", C.amber)
    else ("Flowing", C.green)

  private def fmtRate(n: Double): String =
    if (n >= 1000) {
      val k = n / 1000
      if (k >= 100) s"${math.round(k)}k" else s"${(math.round(k * 10) / 10.0).toString.stripSuffix(".0")}k"
    } else math.round(n).toString

  /** Exact rate for tooltips — no abbreviation, comma-separated */
  private def fmtExact(n: Double): String = f"${math.round(n)}%,d"

  // ── Geometry ──
  private def distributeYs(count: Int, top: Double, bot: Double): Seq[Double] =
    if (count == 0) Seq.empty
    else if (count == 1) Seq((top + bot) / 2.0)
    else {
      val step = (bot - top) / (count - 1)
      (0 until count).map(i => top + step * i)
    }

  private def bezierH(ax: Double, ay: Double, bx: Double, by: Double): String = {
    val dx = (bx - ax) * 0.5
    s"M$ax,$ay C${ax + dx},$ay ${bx - dx},$by $bx,$by"
  }

  private def ribbon(ax: Double, ay: Double, bx: Double, by: Double, w: Double): String = {
    val dx = (bx - ax) * 0.5
    val hh = w / 2
    s"M$ax,${ay - hh} C${ax + dx},${ay - hh} ${bx - dx},${by - hh} $bx,${by - hh} " +
    s"L$bx,${by + hh} C${bx - dx},${by + hh} ${ax + dx},${ay + hh} $ax,${ay + hh} Z"
  }

  private def edgeline(ax: Double, ay: Double, bx: Double, by: Double, off: Double): String = {
    val dx = (bx - ax) * 0.5
    s"M$ax,${ay + off} C${ax + dx},${ay + off} ${bx - dx},${by + off} $bx,${by + off}"
  }

  private def bezierYatX(ax: Double, ay: Double, bx: Double, by: Double, targetX: Double): Double = {
    val dx = (bx - ax) * 0.5
    val px = Array(ax, ax + dx, bx - dx, bx)
    val py = Array(ay, ay, by, by)
    def b(p: Array[Double], t: Double): Double = {
      val u = 1 - t; u * u * u * p(0) + 3 * u * u * t * p(1) + 3 * u * t * t * p(2) + t * t * t * p(3)
    }
    var lo = 0.0; var hi = 1.0
    var i = 0; while (i < 26) { val m = (lo + hi) / 2; if (b(px, m) < targetX) lo = m else hi = m; i += 1 }
    b(py, (lo + hi) / 2)
  }

  // ── Tooltip helpers ──
  private def tipOn(el: dom.Element, buildHtml: () => String): Unit =
    LandingTooltip.attachToElement(el, buildHtml)

  private def tipHeader(t: String): String = LandingTooltip.header(t)
  private def tipKv(rows: String): String = LandingTooltip.kvTable(rows)
  private def tipRow(l: String, v: String, c: Option[String] = None): String = LandingTooltip.kvRow(l, v, c)
  private def tipNote(s: String): String = {
    val safe = LandingTooltip.escape(s)
    s"""<div style="margin-top:10px;font-size:12px;line-height:1.45;color:#c2c8de;">$safe</div>"""
  }

  // ── Ribbon thickness ──
  private def thick(rate: Double, rateRef: Double): Double = {
    val t = math.max(0, math.min(1, rate / math.max(rateRef, 1)))
    (4 + 38 * math.sqrt(t)) * 0.85
  }

  // ── Type icon (uses real SVG/PNG icons from ServiceIcons when available, falls back to monogram) ──
  private def typeIcon(svg: js.Dynamic, cx: Double, cy: Double, sz: Double, typeStr: String): Unit = {
    val r = sz / 2
    svg
      .append("rect")
      .attr("x", cx - r - 4)
      .attr("y", cy - r - 4)
      .attr("width", sz + 8)
      .attr("height", sz + 8)
      .attr("rx", 7)
      .attr("fill", "#eef1f7")
    // Try to use the real icon from ServiceIcons (same icons as Streams page, System Overview, etc.)
    ServiceIcons.forType(typeStr) match {
      case Some(iconUrl) =>
        (svg
          .append("image")
          .attr("href", iconUrl)
          .attr("x", cx - r)
          .attr("y", cy - r)
          .attr("width", sz)
          .attr("height", sz)
          .attr("preserveAspectRatio", "xMidYMid meet")): Unit
      case None =>
        // Fallback: monogram letter
        val mono = typeStr match {
          case t if t.contains("StdOut") || t.contains("StandardOut") => ">_"
          case t if t.contains("Drop") => "∅"
          case t if t.contains("Log") => "≡"
          case t if t.contains("SSE") || t.contains("ServerSent") => "SE"
          case t if t.contains("Websocket") || t.contains("WebSocket") => "WS"
          case t if t.contains("Delta") => "Δ"
          case t if t.contains("Postgres") => "Pg"
          case t if t.contains("Stdin") || t.contains("StandardInput") => ">_"
          case "unknown" | "" => "·"
          case t => t.take(2)
        }
        (svg
          .append("text")
          .attr("x", cx)
          .attr("y", cy + sz * 0.19)
          .attr("text-anchor", "middle")
          .attr("fill", C.ink)
          .attr("font-size", sz * 0.46)
          .attr("font-weight", 800)
          .attr("font-family", "Inter,sans-serif")
          .text(mono)): Unit
    }
  }

  // ── Ellipsize ──
  private def ellipsize(t: String, maxChars: Int): String =
    if (t.length <= maxChars) t else t.take(math.max(1, maxChars - 1)).stripSuffix(" ") + "…"

  // ══════════════════════════════════════════════════════════════════════
  // Component entry point
  // ══════════════════════════════════════════════════════════════════════

  private val PollIntervalMs = 5000 // matches QuineApiClient.PollIntervalMs
  private val StorageKey = "bpc_lastRefresh"
  private val NamespacesKey = "bpc_namespaces"
  private val ScopeKey = "bpc_scope"

  /** The persisted form of [[BackpressureService.Scope.Cluster]]; any other stored value is a
    * member index.
    */
  private val ClusterScope = "cluster"

  /** Persisted forms of the two namespace selections that are not a list of names. Neither can be
    * written as the empty string: `loadStorage` cannot tell that apart from nothing stored at all.
    */
  private val AllGraphs = "*"
  private val NoGraphs = "-"
  private var cachedSvgHtml: String = "" // persists across mount/unmount to avoid flash on tab switch
  // Sticky cache: once hasEnrichment is true for a pipeline, it stays true (config doesn't change at runtime)
  private var enrichmentCache: Set[String] = Set.empty // pipeline names with enrichment

  /** Read a string from localStorage, returning None if absent or on error. */
  private def loadStorage(key: String): Option[String] =
    scala.util.Try(Option(dom.window.localStorage.getItem(key)).filter(_.nonEmpty)).getOrElse(None)

  /** Write a string to localStorage, silently ignoring errors. */
  private def saveStorage(key: String, value: String): Unit = {
    val _ = scala.util.Try(dom.window.localStorage.setItem(key, value))
  }

  /** @param service       the backpressure slice: the resolved view, the user's lookback and scope
    *                       selections, and the dispatch that changes them. The component declares
    *                       only this slice, so it cannot send another capability's commands.
    * @param clusterSignal If defined, the diagram shows a cluster card above the Q node.
    *                       Only present when running Quine Enterprise with cluster target size > 1.
    * @param showScopePicker Whether to render the scope-picker bar above the diagram: the
    *                       cluster-member chips and the named-graph filter. Off by default — with a
    *                       single member and a single graph there is nothing to pick, so the bar has
    *                       nothing to offer. Callers that can have several of either enable it.
    */
  def apply(
    service: BackpressureService,
    clusterSignal: Option[Signal[Pot[V2ServiceStatus]]] = None,
    showScopePicker: Boolean = false,
  ): HtmlElement = {
    var lastRefreshMs: Double = {
      val stored = dom.window.localStorage.getItem(StorageKey)
      if (stored != null && stored.nonEmpty) scala.util.Try(stored.toDouble).getOrElse(js.Date.now())
      else js.Date.now()
    }
    val refreshTextVar = Var(s"every ${PollIntervalMs / 1000}s")
    val refreshColorVar = Var(C.green)

    // Namespace filter. Three states, not two: `None` is every graph, and `Some(empty)` is the user
    // having deselected them all — an empty diagram, which is a thing they are allowed to ask for.
    // Persisted with a sentinel for each, because localStorage cannot distinguish an empty string
    // from an absent one, and both of those states would otherwise serialize to "".
    val storedNamespaces: Option[Set[String]] = loadStorage(NamespacesKey) match {
      case None => None // nothing stored: all graphs
      case Some(AllGraphs) => None
      case Some(NoGraphs) => Some(Set.empty)
      case Some(raw) => Some(raw.split(',').filter(_.nonEmpty).toSet)
    }
    val namespacesVar: Var[Option[Set[String]]] = Var(storedNamespaces)
    val allNamespacesVar: Var[Seq[String]] = Var(Seq.empty) // discovered from the view
    val nsDropdownOpenVar = Var(false)

    def saveNamespaces(sel: Option[Set[String]]): Unit =
      saveStorage(
        NamespacesKey,
        sel match {
          case None => AllGraphs
          case Some(set) if set.isEmpty => NoGraphs
          case Some(set) => set.mkString(",")
        },
      )

    /** Shown when there is no filter (all graphs), or when this graph is in the explicit selection. */
    def nsSelected(sel: Option[Set[String]], ns: String): Boolean = sel.forall(_.contains(ns))

    // Scope is the store's state, not this component's: it changes what the data *means*, so it
    // belongs with the data. The component reads it back to render the picker and sends commands to
    // change it — it never holds its own copy of the selection.
    val scopeSignal: Signal[BackpressureService.Scope] = service.backpressureScopeSignal
    val membersSignal: Signal[Seq[Int]] = service.listClusterMembers
    val memberStatusSignal: Signal[Map[Int, MemberStatus]] = service.memberStatusSignal

    // Local mirror for the pause button's own styling, synced from the store's authoritative paused
    // signal on mount (see the onMountBind below). The freeze itself lives in the store, which keeps
    // recording while frozen so resuming is instant — and which outlives this component, so the mirror
    // must read the store's state back rather than assuming "live" every time the diagram remounts.
    val pausedVar = Var(false)

    // ── Member picker: scroll state ──
    // The "ID:" jump box appears only when the chips overflow; soft shadows mark whichever edge
    // still hides chips; and the selected (or worst) member is scrolled into view. All of that needs
    // the live scroll container and the current selection, read synchronously — hence the mirrors.
    var chipsContainerRef: Option[dom.HTMLElement] = None
    var selectedMember: Option[Int] = None
    var didInitialScroll = false
    val chipsOverflowVar: Var[Boolean] = Var(false)
    val chipsAtStartVar: Var[Boolean] = Var(true)
    val chipsAtEndVar: Var[Boolean] = Var(true)
    val memberInputVar: Var[String] = Var("")

    // Drag-to-pan the chip row. The scrollbar is hidden (bp-hide-scrollbar), so on a plain mouse this
    // is how the strip scrolls at all. `dragMoved` suppresses the chip click that would otherwise fire
    // when a drag happens to end on a chip.
    var dragActive = false
    var dragStartX = 0.0
    var dragStartScroll = 0.0
    var dragMoved = false

    def updateChipsScrollState(): Unit = chipsContainerRef.foreach { el =>
      chipsOverflowVar.set(el.scrollWidth > el.clientWidth + 1)
      chipsAtStartVar.set(el.scrollLeft <= 1)
      chipsAtEndVar.set(el.scrollLeft + el.clientWidth >= el.scrollWidth - 1)
    }

    // Layout width can settle a frame or two after mount; re-measure then too.
    def scheduleChipsMeasure(): Unit = {
      updateChipsScrollState()
      val _ = dom.window.requestAnimationFrame((_: Double) => updateChipsScrollState())
      val _ = dom.window.setTimeout(() => updateChipsScrollState(), 150.0)
    }

    def scrollMemberIntoView(idx: Int): Unit = chipsContainerRef.foreach { el =>
      Option(el.querySelector(s"[data-member='$idx']")).foreach { node =>
        val chip = node.asInstanceOf[dom.HTMLElement]
        val chipRect = chip.getBoundingClientRect()
        val contRect = el.getBoundingClientRect()
        if (chipRect.left < contRect.left) el.scrollLeft -= (contRect.left - chipRect.left) + 8
        else if (chipRect.right > contRect.right) el.scrollLeft += (chipRect.right - contRect.right) + 8
        updateChipsScrollState()
      }
    }
    def scrollSelectedIntoView(): Unit = selectedMember.foreach(scrollMemberIntoView)

    /** Ask the store to change scope, and remember the choice.
      *
      * The reply is the scope that actually took effect — a member that has left the cluster falls
      * back to the whole cluster — so what gets persisted is what is really being shown, not what was
      * asked for. A selection that has become impossible must not be able to outlive the cluster it
      * named by sitting in localStorage.
      */
    def selectScope(scope: BackpressureService.Scope): Unit =
      service.backpressureDispatch.onNext(
        BackpressureService.SetScope(
          scope,
          replyTo = Observer[BackpressureService.Scope] { applied =>
            if (applied != scope)
              dom.console.warn(s"[Backpressure] $scope is no longer in the cluster; showing $applied instead")
            saveStorage(
              ScopeKey,
              applied match {
                case BackpressureService.Scope.Cluster => ClusterScope
                case BackpressureService.Scope.Member(idx) => idx.toString
              },
            )
          },
        ),
      )

    /** Restore the previous scope on mount. The store validates it against the members that actually
      * exist, so a member that has left the cluster since the page was last open falls back to the
      * whole cluster rather than rendering an empty diagram.
      */
    def restoreScope(): Unit = loadStorage(ScopeKey).foreach {
      case ClusterScope => selectScope(BackpressureService.Scope.Cluster)
      case idx => idx.toIntOption.foreach(i => selectScope(BackpressureService.Scope.Member(i)))
    }

    def updateRefreshDisplay(): Unit = {
      val age = js.Date.now() - lastRefreshMs
      val staleThresholdMs = PollIntervalMs * 2.1 // 110% of poll interval
      if (age > staleThresholdMs) {
        val secs = (age / 1000).toInt
        val timeStr =
          if (secs < 90) s"${secs}s ago"
          else if (secs < 7200) s"${secs / 60}m ago"
          else s"${secs / 3600}h ago"
        refreshTextVar.set(timeStr)
        val color = if (secs >= 60) C.red else if (secs >= 30) C.amber else C.yellow
        refreshColorVar.set(color)
      } else {
        refreshTextVar.set(s"every ${PollIntervalMs / 1000}s")
        refreshColorVar.set(C.green)
      }
    }

    // The view arrives already scoped, windowed, and aggregated — rates, level summaries and
    // per-member attribution are all the store's work, not this component's. Pausing is the
    // store's too: it freezes what it emits while history keeps accumulating underneath, so there
    // is no "last good" copy to hold here and no mutable rate bookkeeping to corrupt if Airstream
    // replays a value.
    val viewSignal: Signal[Option[BackpressureView]] = service.backpressureSnapshotSignal.map { pot =>
      // Only mark a refresh on genuinely fresh data, and not while frozen — the staleness readout
      // should show how old the frozen view is.
      pot match {
        case Pot.Ready(_) if !pausedVar.now() =>
          lastRefreshMs = js.Date.now()
          scala.util.Try(dom.window.localStorage.setItem(StorageKey, lastRefreshMs.toLong.toString))
          updateRefreshDisplay()
        case _ => ()
      }
      val view = pot.toOption
      // Discover the graphs from the view, and drop any stored selection naming one that no longer
      // exists — a filter pinned to a deleted graph would quietly hide everything. `v.namespaces` is
      // the authoritative list (every graph, including idle ones); the pipelines are folded in too so
      // a graph is never dropped from the list on the rare poll its own namespace entry is missing.
      view.foreach { v =>
        val nss =
          (v.namespaces ++ v.ingests.map(_.namespace) ++ v.standingQueries.map(_.namespace)).distinct.sorted
        if (nss != allNamespacesVar.now()) {
          allNamespacesVar.set(nss)
          namespacesVar.now().foreach { current =>
            val valid = current.intersect(nss.toSet)
            if (valid != current) {
              // Still `Some`, even when nothing valid survives: the user selected specific graphs,
              // and none of them exist any more. That is "no graphs", not "all graphs" — silently
              // widening a selection to everything is the one outcome they did not ask for.
              val updated = Some(valid)
              namespacesVar.set(updated)
              saveNamespaces(updated)
            }
          }
        }
      }
      view
    }

    // Cluster info — updated from the optional cluster signal
    val clusterInfoVar: Var[Option[ClusterInfo]] = Var(None)

    // Heartbeat timer to update staleness display every second
    var heartbeatHandle: Int = 0
    // Whether the chips overflow depends on the width they are given, which changes with the window.
    val onResize: js.Function1[dom.Event, Unit] = (_: dom.Event) => updateChipsScrollState()
    // Drag-to-pan lives on the document so a drag keeps tracking even once the pointer leaves the row.
    val onDragMove: js.Function1[dom.MouseEvent, Unit] = (ev: dom.MouseEvent) =>
      if (dragActive) chipsContainerRef.foreach { el =>
        val dx = ev.clientX - dragStartX
        if (math.abs(dx) > 3) dragMoved = true
        el.scrollLeft = dragStartScroll - dx
        updateChipsScrollState()
      }
    val onDragUp: js.Function1[dom.MouseEvent, Unit] = (_: dom.MouseEvent) => dragActive = false

    div(
      width := "100%",
      styleAttr := "font-family:Inter,system-ui,sans-serif;color:#0a295b;",
      onMountCallback { _ =>
        heartbeatHandle = dom.window.setInterval(() => updateRefreshDisplay(), 1000)
        dom.window.addEventListener("resize", onResize)
        dom.window.addEventListener("mousemove", onDragMove)
        dom.window.addEventListener("mouseup", onDragUp)
        restoreScope()
      },
      onUnmountCallback { _ =>
        if (heartbeatHandle != 0) dom.window.clearInterval(heartbeatHandle)
        dom.window.removeEventListener("resize", onResize)
        dom.window.removeEventListener("mousemove", onDragMove)
        dom.window.removeEventListener("mouseup", onDragUp)
      },
      // Mirror the selected scope, so the chip row can scroll it into view on mount without having to
      // wait for a signal it is not otherwise subscribed to.
      onMountBind { (_: MountContext[HtmlElement]) =>
        scopeSignal --> Observer[BackpressureService.Scope] {
          case BackpressureService.Scope.Member(idx) => selectedMember = Some(idx)
          case BackpressureService.Scope.Cluster => selectedMember = None
        }
      },
      // Sync the pause mirror from the store's authoritative freeze. On mount this emits the current
      // value, so a diagram that remounts (an in-app page change and back) reflects the real state
      // instead of resetting to "live" over a view the store is still holding frozen.
      onMountBind { (_: MountContext[HtmlElement]) =>
        service.backpressurePausedSignal --> pausedVar.writer
      },
      // The cluster's identity, from both of the things that know about it.
      //
      // The status endpoint is authoritative and is the only source of hot spares (which never run
      // an ingest, so never appear in a backpressure snapshot) and of health. But it is optional: on
      // OSS there is no such endpoint, and on Enterprise the signal is withheld from a user without
      // permission to read cluster status. The members that answered the backpressure poll are
      // always known, so they carry the member count on their own when the status signal cannot.
      //
      // Combining both — rather than subscribing to the status alone — is also what makes a member
      // joining or leaving show up promptly, instead of waiting for the status signal to re-emit.
      onMountBind { (_: MountContext[HtmlElement]) =>
        val statusSignal: Signal[Option[V2ServiceStatus]] =
          clusterSignal.fold(Signal.fromValue(Option.empty[V2ServiceStatus]): Signal[Option[V2ServiceStatus]])(
            _.map(_.toOption),
          )
        membersSignal.combineWith(statusSignal) --> Observer[(Seq[Int], Option[V2ServiceStatus])] {
          case (members, statusOpt) =>
            val update: Option[ClusterInfo] = statusOpt match {
              case Some(status) if status.cluster.targetSize > 1 =>
                Some(
                  ClusterInfo(
                    memberCount = status.cluster.clusterMembers.size,
                    hotSpareCount = Some(status.cluster.hotSpares.size),
                    fullyUp = Some(status.fullyUp),
                  ),
                )
              // No status to read, but the poll is hearing from several members — so there is a
              // cluster, and we can say how big it is and nothing more.
              case _ if members.sizeIs > 1 =>
                Some(ClusterInfo(memberCount = members.size, hotSpareCount = None, fullyUp = None))
              case _ => None
            }
            clusterInfoVar.set(update)
        }
      },
      // ── Status bar (HTML, above the SVG) ──
      div(
        styleAttr := "display:flex;align-items:stretch;gap:0;background:#ffffff;border:1px solid rgba(10,41,91,0.1);border-radius:14px;margin-bottom:14px;box-shadow:0 6px 22px rgba(10,41,91,0.07);",
        child <-- viewSignal.map {
          case None => div(styleAttr := "padding:13px 20px;color:#6c7390;", "Loading...")
          case Some(view) =>
            val nsF = namespacesVar.now()
            def nsVis(ns: String): Boolean = nsSelected(nsF, ns)
            val filtered = view.copy(
              ingests = view.ingests.filter(p => nsVis(p.namespace)),
              standingQueries = view.standingQueries.filter(sq => nsVis(sq.namespace)),
            )
            val ingests0 = filtered.ingests
            val bn = computeBottleneck(filtered)
            // Scoped to one member that is reporting nothing: the same condition the diagram body uses
            // to draw the no-data view. Every metric here would be from a stale last-known snapshot, so
            // none is stated — the cells show an em-dash instead of a number that is no longer true.
            val selNoData = !view.isAggregate && view.hostsReporting.isEmpty && (view.scope match {
              case BackpressureService.Scope.Member(_) => true
              case BackpressureService.Scope.Cluster => false
            })
            def dash(el: HtmlElement): HtmlElement =
              if (selNoData) span(styleAttr := "color:#9aa0b8;font-weight:600;", "—") else el
            // The status word stays a flow-control statement: "Stopped" means the ingest valve is
            // closed, never that a member is gone. Member liveness is a different axis and is reported
            // separately (the "N of M reporting" line, and per-member the red chip / no-data view) —
            // because the only member-liveness signal we have is a member's snapshot being absent, and
            // "absent" is not "stopped". "No Data" here is the whole view being that one absent member.
            val (sLabel, sColor) =
              if (selNoData) ("No Data", C.gray)
              else systemStatus(view, bn)
            // Rates are already windowed and summed across the members in scope.
            val totalRate = ingests0.map(_.rate).sum
            val open = view.globalValve.combined.isOpen
            div(
              styleAttr := "display:flex;align-items:stretch;width:100%;",
              // System status
              div(
                styleAttr := "flex:1;display:flex;align-items:center;gap:11px;padding:13px 20px;border-right:1px solid rgba(10,41,91,0.08);",
                span(
                  styleAttr := s"width:11px;height:11px;border-radius:50%;background:$sColor;box-shadow:0 0 10px $sColor;",
                ),
                div(
                  div(
                    styleAttr := "font-size:11px;letter-spacing:0.16em;text-transform:uppercase;color:#6c7390;white-space:nowrap;",
                    "System status",
                  ),
                  div(
                    styleAttr := s"font-size:17px;font-weight:600;color:$sColor;line-height:1.15;margin-top:2px;",
                    sLabel,
                  ),
                  // Liveness, kept off the status word: an aggregate is computed only from the members
                  // still reporting, so it can read "Flowing" while some are missing. This says how many
                  // of the members in scope actually answered the latest poll. Shown only when some did
                  // not — and only when aggregating, since a single-member scope that is not reporting is
                  // already the whole "No Data" story above. (A cluster that has gone entirely dark is a
                  // separate signal still: the Update indicator ages toward red as no poll succeeds.)
                  if (view.isAggregate && view.hasMissingHosts)
                    div(
                      styleAttr := s"font-size:11px;font-weight:500;color:${C.amber};line-height:1.15;margin-top:2px;",
                      s"${view.hostsReporting.size} of ${view.hostsInScope.size} reporting",
                    )
                  else emptyNode,
                ),
              ),
              // Throughput
              div(
                styleAttr := "flex:1;display:flex;flex-direction:column;justify-content:center;padding:13px 20px;border-right:1px solid rgba(10,41,91,0.08);cursor:default;",
                onMountCallback { ctx =>
                  LandingTooltip.attachToElement(
                    ctx.thisNode.ref,
                    () => {
                      val perIngest = ingests0.map { p =>
                        tipRow(p.name, s"${fmtExact(p.rate)} ev/s")
                      }.mkString
                      tipHeader("Combined ingest throughput") +
                      tipKv(perIngest + tipRow("Total", s"${fmtExact(totalRate)} ev/s"))
                    },
                  )
                },
                div(
                  styleAttr := "font-size:11px;letter-spacing:0.16em;text-transform:uppercase;color:#6c7390;",
                  "Throughput",
                ),
                dash(
                  div(
                    styleAttr := "font-size:17px;font-weight:600;line-height:1.15;margin-top:2px;",
                    span(styleAttr := "color:#1658b7;font-weight:700;", fmtRate(totalRate)),
                    span(styleAttr := "color:#6c7390;font-weight:500;font-size:13px;", " / s"),
                  ),
                ),
              ),
              // Bottleneck — omitted whenever the figures on screen were reduced across more than one
              // member. The bottleneck names a single stage, but an aggregate has already thrown away
              // which member that stage is stuck on: "Output: kafka-out" over a 20-member cluster is a
              // statement the user cannot act on, and one they would reasonably read as "on all of
              // them". The member picker's tints are the cluster-scale answer to the same question,
              // and clicking through to a member gets the bottleneck back, now attributable.
              //
              // The test is what was reduced, NOT the selected scope: OSS and a single-node cluster
              // resolve Scope.Cluster over exactly one host, aggregate nothing, and must keep it. A
              // no-data member has no bottleneck to name either.
              if (filtered.isAggregate || selNoData) emptyNode
              else
                div(
                  styleAttr := "display:flex;flex-direction:column;justify-content:center;padding:13px 20px;border-right:1px solid rgba(10,41,91,0.08);flex:1;min-width:0;",
                  div(
                    styleAttr := "font-size:11px;letter-spacing:0.16em;text-transform:uppercase;color:#6c7390;",
                    "Bottleneck",
                  ),
                  bn match {
                    case Some(bp) =>
                      val parts = bp.label.split(": ", 2)
                      val (stage, name) = if (parts.length == 2) (parts(0), parts(1)) else ("", bp.label)
                      div(
                        styleAttr := "line-height:1.15;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;",
                        span(styleAttr := "font-size:13px;font-weight:500;color:#6c7390;", s"$stage: "),
                        span(styleAttr := "font-size:17px;font-weight:600;color:#0a295b;", name),
                      )
                    case None =>
                      div(
                        styleAttr := "font-size:14px;font-weight:500;color:#6c7390;line-height:1.15;margin-top:2px;",
                        "None",
                      )
                  },
                ),
              // Global valve
              {
                val rc = view.globalValve.combined.oneMinuteClosures
                val rcColor = if (rc > 10) C.red else "#0a295b"
                div(
                  styleAttr := "flex:1;display:flex;flex-direction:column;justify-content:center;padding:13px 20px;",
                  div(
                    styleAttr := "font-size:11px;letter-spacing:0.16em;text-transform:uppercase;color:#6c7390;white-space:nowrap;",
                    "Global ingest valve",
                  ),
                  dash(
                    div(
                      styleAttr := "font-size:17px;font-weight:600;line-height:1.15;margin-top:2px;",
                      span(styleAttr := s"color:${if (open) C.green else C.red};", if (open) "Open" else "Closed"),
                      span(styleAttr := "color:#6c7390;font-weight:500;", " · "),
                      span(styleAttr := s"color:$rcColor;font-weight:700;", rc.toString),
                      span(styleAttr := "color:#6c7390;font-weight:500;font-size:13px;", " closed / min"),
                    ),
                  ),
                )
              },
              // Update section
              div(
                styleAttr := "flex:1;display:flex;flex-direction:column;justify-content:center;align-items:flex-end;padding:13px 20px;border-left:1px solid rgba(10,41,91,0.08);cursor:default;min-width:140px;",
                onMountCallback { ctx =>
                  LandingTooltip.attachToElement(
                    ctx.thisNode.ref,
                    () => {
                      val d = new js.Date(lastRefreshMs)
                      tipHeader("Data freshness") +
                      tipKv(
                        tipRow("Last update", d.toLocaleTimeString()) +
                        tipRow("Poll interval", s"${PollIntervalMs / 1000}s") +
                        tipRow("Status", if (pausedVar.now()) "Paused" else "Live"),
                      ) +
                      tipNote(
                        "Polls the backpressure API on a fixed interval. Indicator turns yellow, amber, then red as data grows stale.",
                      )
                    },
                  )
                },
                div(
                  styleAttr := "font-size:11px;letter-spacing:0.16em;text-transform:uppercase;color:#6c7390;white-space:nowrap;",
                  "Update",
                ),
                div(
                  styleAttr := "display:flex;align-items:center;gap:6px;margin-top:2px;",
                  span(
                    styleAttr <-- pausedVar.signal.combineWith(refreshColorVar.signal).map { case (paused, c) =>
                      val color = if (paused) C.gray else c
                      s"font-size:14px;font-weight:400;color:${if (color == C.green) "#6c7390" else color};white-space:nowrap;line-height:1.15;"
                    },
                    child <-- pausedVar.signal.combineWith(refreshTextVar.signal).map { case (paused, txt) =>
                      val c = refreshColorVar.now()
                      if (paused) span("paused")
                      else if (c == C.green) span(txt)
                      else span("last update ", span(txt))
                    },
                  ),
                  span(
                    styleAttr <-- pausedVar.signal.combineWith(refreshColorVar.signal).map { case (paused, c) =>
                      val color = if (paused) C.gray else c
                      s"width:7px;height:7px;border-radius:50%;background:$color;flex-shrink:0;"
                    },
                  ),
                  // Pause/Play button — aligned with content row. The freeze itself happens in the
                  // store (which keeps recording while frozen, so the window stays whole and
                  // resuming is instant); this only mirrors the state for the button's own styling.
                  div(
                    styleAttr := "cursor:pointer;padding:2px;margin-left:4px;display:flex;align-items:center;",
                    onClick.stopPropagation --> { _ =>
                      // Only dispatch; the store flips its paused flag and the mirror follows it
                      // through backpressurePausedSignal, keeping one writer for the button state.
                      val nowPaused = !pausedVar.now()
                      service.backpressureDispatch.onNext(
                        if (nowPaused)
                          BackpressureService.PauseUpdates(
                            replyTo = Observer[Unit](_ => dom.console.log("[Backpressure] Updates paused")),
                          )
                        else
                          BackpressureService.ResumeUpdates(
                            replyTo = Observer[Unit](_ => dom.console.log("[Backpressure] Updates resumed")),
                          ),
                      )
                    },
                    child <-- pausedVar.signal.map { paused =>
                      if (paused)
                        svg.svg(
                          svg.width := "14",
                          svg.height := "14",
                          svg.viewBox := "0 0 16 16",
                          svg.polygon(svg.points := "4,2 4,14 13,8", svg.fill := C.brite),
                        )
                      else
                        svg.svg(
                          svg.width := "14",
                          svg.height := "14",
                          svg.viewBox := "0 0 16 16",
                          svg.rect(
                            svg.x := "3",
                            svg.y := "2",
                            svg.width := "4",
                            svg.height := "12",
                            svg.rx := "1",
                            svg.fill := "#6c7390",
                          ),
                          svg.rect(
                            svg.x := "9",
                            svg.y := "2",
                            svg.width := "4",
                            svg.height := "12",
                            svg.rx := "1",
                            svg.fill := "#6c7390",
                          ),
                        )
                    },
                  ),
                ),
              ),
            )
        },
      ),
      // ── Scope picker (HTML, between the status bar and the SVG) ──
      // A pinned "Cluster" chip, then a scrolling row of member chips, each tinted by that member's
      // own worst state. Its job is triage: at cluster scale the diagram cannot say which member is
      // stuck (see the Bottleneck compartment), so this row says it instead, and clicking a chip
      // scopes the whole diagram to that member — where the bottleneck comes back, attributable.
      //
      // The graph filter is pinned to the right of the same row. Unlike the chips it is NOT gated on
      // there being several members: a single-node deployment with several named graphs still needs
      // to filter them, and hiding the control with the chips would take it away from exactly the
      // deployments most likely to be running a handful of graphs on one host.
      child <-- membersSignal.combineWith(allNamespacesVar.signal).map { case (members, allNs) =>
        val showChips = showScopePicker && members.sizeIs > 1
        // Show the graph list whenever any graph exists — even a lone graph, so a deployment can see
        // which graphs it has. Gated on `showScopePicker`: where only one member and one graph can
        // ever exist, the bar has nothing to pick and is omitted entirely rather than shown empty.
        val showGraphs = showScopePicker && allNs.nonEmpty
        if (!showChips && !showGraphs) emptyNode
        else {
          // Jump to the typed member. The box is deliberately not cleared: the Enter handler
          // re-selects its text so numbers can be typed in rapid succession to hop between members.
          def goToMember(): Unit =
            memberInputVar.now().trim.toIntOption match {
              case Some(n) if members.contains(n) =>
                selectScope(BackpressureService.Scope.Member(n))
                scrollMemberIntoView(n)
              case _ => () // unknown or out of range: leave it for the user to correct
            }
          div(
            styleAttr := "display:flex;align-items:center;background:#ffffff;border:1px solid rgba(10,41,91,0.1);border-radius:12px;margin-bottom:14px;box-shadow:0 3px 12px rgba(10,41,91,0.05);",
            // Pinned: the whole-cluster aggregate
            if (!showChips) emptyNode
            else
              div(
                styleAttr <-- scopeSignal.map { scope =>
                  val base =
                    "flex-shrink:0;padding:10px 16px;cursor:pointer;font-size:13px;font-weight:700;white-space:nowrap;border-right:1px solid rgba(10,41,91,0.08);border-top-left-radius:11px;border-bottom-left-radius:11px;"
                  if (scope == BackpressureService.Scope.Cluster) base + "color:#fff;background:var(--cui-primary);"
                  else base + "color:var(--cui-primary);background:transparent;"
                },
                onClick --> { _ => selectScope(BackpressureService.Scope.Cluster) },
                "Cluster",
              ),
            if (!showChips) emptyNode
            else
              div(
                styleAttr := "flex-shrink:0;padding:10px 2px 10px 14px;font-size:12px;font-weight:600;color:#6c7390;white-space:nowrap;",
                "Member:",
              ),
            // Scrolling member chips, wrapped so the scroll shadows can overlay whichever edge still
            // hides chips.
            if (!showChips) emptyNode
            else
              div(
                styleAttr := "flex:1;min-width:0;position:relative;",
                div(
                  cls := "bp-hide-scrollbar",
                  styleAttr := "display:flex;align-items:center;gap:5px;overflow-x:auto;padding:8px 10px;user-select:none;cursor:grab;",
                  onMountCallback { ctx =>
                    chipsContainerRef = Some(ctx.thisNode.ref)
                    scheduleChipsMeasure()
                    scrollSelectedIntoView()
                  },
                  onUnmountCallback(_ => chipsContainerRef = None),
                  onScroll --> { _ => updateChipsScrollState() },
                  // Start a drag-to-pan. The move/up handlers live on the document (above).
                  onMouseDown --> { (ev: dom.MouseEvent) =>
                    chipsContainerRef.foreach { el =>
                      dragActive = true; dragMoved = false
                      dragStartX = ev.clientX; dragStartScroll = el.scrollLeft
                    }
                  },
                  // On first load, bring the first member needing attention into view. With enough
                  // members to overflow the row, the one that is on fire is otherwise off-screen, and
                  // a triage control that hides the thing you are triaging is not one.
                  onMountBind { _ =>
                    memberStatusSignal --> Observer[Map[Int, MemberStatus]] { statuses =>
                      if (!didInitialScroll && statuses.nonEmpty) {
                        didInitialScroll = true
                        members.find(idx => statuses.get(idx).exists(memberNeedsAttention)).foreach { idx =>
                          val _ = dom.window.requestAnimationFrame((_: Double) => scrollMemberIntoView(idx))
                        }
                      }
                    }
                  },
                  members.map { idx =>
                    div(
                      dataAttr("member") := idx.toString,
                      styleAttr <-- scopeSignal.combineWith(memberStatusSignal).map { case (scope, statuses) =>
                        val base =
                          "flex-shrink:0;min-width:24px;text-align:center;padding:3px 9px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;border:1px solid transparent;"
                        if (scope == BackpressureService.Scope.Member(idx))
                          base + s"color:#fff;background:${C.brite};"
                        else {
                          // Muted fill (~17% alpha) with a matching border: legible as a state at a
                          // glance without competing with the diagram itself for attention.
                          val c = statuses.get(idx).map(memberChipColor).getOrElse(C.gray)
                          base + s"color:${C.ink};background:${c}2b;border-color:${c}55;"
                        }
                      },
                      onClick --> { _ =>
                        // A click that was really the end of a drag selects nothing.
                        if (dragMoved) dragMoved = false
                        else {
                          selectScope(BackpressureService.Scope.Member(idx))
                          scrollMemberIntoView(idx)
                        }
                      },
                      idx.toString,
                    )
                  },
                ),
                child <-- chipsAtStartVar.signal.distinct.map { atStart =>
                  if (atStart) emptyNode
                  else
                    div(
                      styleAttr := "position:absolute;left:0;top:0;bottom:0;width:26px;pointer-events:none;" +
                      "background:linear-gradient(to right, rgba(10,41,91,0.16) 0%, rgba(10,41,91,0) 100%);",
                    )
                },
                child <-- chipsAtEndVar.signal.distinct.map { atEnd =>
                  if (atEnd) emptyNode
                  else
                    div(
                      styleAttr := "position:absolute;right:0;top:0;bottom:0;width:26px;pointer-events:none;" +
                      "background:linear-gradient(to left, rgba(10,41,91,0.16) 0%, rgba(10,41,91,0) 100%);",
                    )
                },
              ),
            // Pinned right: jump to a member by number — only once the chips overflow, since below
            // that scrolling is not a chore. `.distinct` keeps a re-emitted scroll state from
            // rebuilding the input and stealing its focus mid-type.
            if (!showChips) emptyNode
            else
              child <-- chipsOverflowVar.signal.distinct.map { overflow =>
                if (!overflow) emptyNode
                else
                  div(
                    styleAttr := "flex-shrink:0;display:flex;align-items:center;gap:6px;" +
                    "padding:8px 12px;border-left:1px solid rgba(10,41,91,0.08);",
                    span(styleAttr := "font-size:12px;color:#6c7390;white-space:nowrap;", "ID:"),
                    input(
                      tpe := "number",
                      placeholder := "#",
                      styleAttr := "width:52px;padding:4px 6px;border:1px solid rgba(10,41,91,0.18);border-radius:6px;font-size:12px;color:#0a295b;",
                      controlled(
                        value <-- memberInputVar.signal,
                        onInput.mapToValue --> memberInputVar,
                      ),
                      onKeyDown --> { ev =>
                        if (ev.key == "Enter") {
                          goToMember()
                          ev.target match {
                            case inp: dom.html.Input =>
                              val _ = dom.window.requestAnimationFrame { (_: Double) =>
                                inp.focus(); inp.select()
                              }
                            case _ => ()
                          }
                        }
                      },
                      onBlur --> { _ => goToMember() },
                      // The native up/down spinner navigates on each click, not only on Enter/blur.
                      onChange --> { _ => goToMember() },
                    ),
                  )
              },
            // Pinned right: the graph filter. NOTE: no flex `gap` on the row — the dropdown's
            // zero-width, out-of-flow wrapper span is a flex child, and a gap would reserve space for
            // it, visibly widening the row whenever the dropdown opens.
            if (!showGraphs) emptyNode
            else
              div(
                // `margin-left:auto` keeps the filter pinned to the right edge of the row even when it
                // is the only control present (single-graph, single-node): with member chips it is a
                // no-op, since their `flex:1` already fills the space to its left.
                styleAttr := "flex-shrink:0;margin-left:auto;position:relative;display:flex;align-items:center;padding:8px 14px;border-left:1px solid rgba(10,41,91,0.08);cursor:pointer;border-top-right-radius:11px;border-bottom-right-radius:11px;",
                onClick.stopPropagation --> { _ => nsDropdownOpenVar.update(!_) },
                child <-- namespacesVar.signal.map { selected =>
                  val label = selected match {
                    case None => "All Graphs"
                    case Some(sel) if allNs.nonEmpty && sel.size == allNs.size => "All Graphs"
                    case Some(sel) if sel.isEmpty => "No Graphs"
                    case Some(sel) if sel.size == 1 => sel.head
                    case Some(sel) => s"${sel.size} Graphs"
                  }
                  span(
                    styleAttr := "font-size:13px;font-weight:600;color:#0a295b;white-space:nowrap;max-width:160px;overflow:hidden;text-overflow:ellipsis;",
                    label,
                  )
                },
                span(styleAttr := "font-size:10px;color:#6c7390;margin-left:6px;", "▾"),
                child.maybe <-- nsDropdownOpenVar.signal.map { isOpen =>
                  if (!isOpen) None
                  else
                    Some(
                      span(
                        // Backdrop to catch clicks outside the dropdown
                        div(
                          styleAttr := "position:fixed;top:0;left:0;width:100vw;height:100vh;z-index:99;",
                          onClick.stopPropagation --> { _ => nsDropdownOpenVar.set(false) },
                        ),
                        div(
                          styleAttr := "position:absolute;top:100%;right:0;margin-top:6px;background:#ffffff;border:1px solid rgba(10,41,91,0.14);border-radius:10px;box-shadow:0 8px 24px rgba(10,41,91,0.12);z-index:100;min-width:200px;padding:8px;",
                          onClick.stopPropagation --> { _ => () },
                          div(
                            styleAttr := "font-size:9px;letter-spacing:0.12em;text-transform:uppercase;color:#6c7390;padding:4px 12px 6px;",
                            "Named Graphs",
                          ),
                          allNs.map { ns =>
                            div(
                              styleAttr := "padding:5px 8px;border-radius:6px;cursor:pointer;font-size:13px;color:#0a295b;display:flex;align-items:center;gap:8px;white-space:nowrap;",
                              child <-- namespacesVar.signal.map { selected =>
                                val isSelected = nsSelected(selected, ns)
                                span(
                                  styleAttr := s"width:14px;height:14px;border-radius:3px;border:1.5px solid ${if (isSelected) C.brite
                                  else "#c2c8de"};display:inline-flex;align-items:center;justify-content:center;font-size:10px;color:${C.brite};flex-shrink:0;",
                                  if (isSelected) "✓" else "",
                                )
                              },
                              span(ns),
                              onClick --> { _ =>
                                // Materialize "all" before toggling one off, so the first click
                                // deselects exactly the one clicked rather than everything else.
                                val effective: Set[String] = namespacesVar.now().getOrElse(allNs.toSet)
                                val toggled = if (effective.contains(ns)) effective - ns else effective + ns
                                // Everything selected collapses back to None (all graphs). An empty
                                // set stays Some(empty) — "no graphs" — rather than wrapping around
                                // to mean "all", which would make the last click do the opposite of
                                // what it says.
                                val updated: Option[Set[String]] =
                                  if (allNs.nonEmpty && toggled.size == allNs.size) None else Some(toggled)
                                namespacesVar.set(updated)
                                saveNamespaces(updated)
                              },
                            )
                          },
                          div(
                            styleAttr := "padding:6px 8px 2px;",
                            span(
                              styleAttr <-- namespacesVar.signal.map { sel =>
                                val isAll = sel.isEmpty
                                s"font-size:12px;cursor:pointer;color:${if (isAll) "#c2c8de"
                                else C.brite};font-weight:500;${if (isAll) "" else "text-decoration:underline;"}"
                              },
                              child.text <-- namespacesVar.signal
                                .map(sel => if (sel.isEmpty) "All selected" else "Select all"),
                              onClick --> { _ => namespacesVar.set(None); saveNamespaces(None) },
                            ),
                          ),
                        ),
                      ),
                    )
                },
              ),
          )
        }
      },
      // ── SVG diagram ──
      div(
        minHeight := "300px",
        onMountCallback { ctx =>
          val container = ctx.thisNode.ref
          // Restore cached SVG immediately on mount to avoid flash
          if (container.childElementCount == 0 && cachedSvgHtml.nonEmpty) {
            container.innerHTML = cachedSvgHtml
            container.querySelector("svg") match {
              case svg: dom.Element => container.style.minHeight = s"${svg.getAttribute("height")}px"
              case _ =>
            }
          }
        },
        // One observer over every input: the diagram is a pure function of (view, graph filter,
        // cluster info), so a change to any of them redraws it. Cluster info in particular arrives
        // asynchronously and often *after* the first view — reading it with `.now()` at render time
        // (as this did) meant the cluster identity simply did not appear until the next poll happened
        // to redraw. Combining the signal is what fixes that; it is also why there is no need for a
        // held copy of the last render to replay.
        onMountBind { ctx =>
          val container = ctx.thisNode.ref
          viewSignal.combineWith(namespacesVar.signal, clusterInfoVar.signal) -->
          Observer[(Option[BackpressureView], Option[Set[String]], Option[ClusterInfo])] {
            case (None, _, _) => ()
            case (Some(view), namespaces, clusterInfo) =>
              // Render into a detached div, then move DOM nodes (preserving event listeners)
              val offscreen = dom.document.createElement("div").asInstanceOf[dom.HTMLElement]
              renderDiagram(offscreen, view, clusterInfo, namespaces)
              offscreen.querySelector("svg") match {
                case svg: dom.Element =>
                  container.style.minHeight = s"${svg.getAttribute("height")}px"
                case _ =>
              }
              // Swap by moving child nodes — preserves event listeners attached by tipOn()
              while (container.firstChild != null) container.removeChild(container.firstChild)
              while (offscreen.firstChild != null) container.appendChild(offscreen.firstChild)
              cachedSvgHtml = container.innerHTML
          }
        },
      ),
      // ── Legend (HTML, below the SVG) ──
      div(
        styleAttr := "display:flex;align-items:center;gap:18px;flex-wrap:wrap;margin-top:-4px;padding:10px 20px 16px;font-size:12.5px;color:#6c7390;",
        div(
          styleAttr := "display:flex;align-items:center;gap:8px;",
          span(
            styleAttr := "width:22px;height:7px;border-radius:2px;background:linear-gradient(90deg,#1658b7,#4a82d8);",
          ),
          "Flow · width = events/sec",
        ),
        div(
          styleAttr := "display:flex;align-items:center;gap:9px;",
          "Stage state:",
          span(
            styleAttr := "display:flex;align-items:center;gap:5px;",
            span(styleAttr := s"width:9px;height:9px;border-radius:50%;background:${C.green};"),
            "flowing",
          ),
          span(
            styleAttr := "display:flex;align-items:center;gap:5px;",
            span(styleAttr := s"width:9px;height:9px;border-radius:50%;background:${C.yellow};"),
            "constrained",
          ),
          span(
            styleAttr := "display:flex;align-items:center;gap:5px;",
            span(styleAttr := s"width:9px;height:9px;border-radius:50%;background:${C.amber};"),
            "backpressured",
          ),
          span(
            styleAttr := "display:flex;align-items:center;gap:5px;",
            span(styleAttr := s"width:9px;height:9px;border-radius:50%;background:${C.red};"),
            "stopped",
          ),
        ),
        // Only where the diagram can actually mark one — an aggregate view never does.
        child <-- viewSignal.map { opt =>
          if (opt.exists(_.isAggregate)) emptyNode
          else
            div(
              styleAttr := "display:flex;align-items:center;gap:8px;",
              span(styleAttr := s"width:22px;height:7px;border-radius:2px;background:${C.amber};"),
              "Bottleneck",
            )
        },
        child <-- viewSignal.map { opt =>
          val hasAck = opt.exists(_.ingests.exists(_.postGraphWrite.isDefined))
          if (hasAck)
            div(
              styleAttr := "display:flex;align-items:center;gap:8px;",
              span(
                styleAttr := s"width:18px;height:7px;border-radius:2px;background:repeating-linear-gradient(90deg,${C.green} 0 4px,transparent 4px 9px);",
              ),
              "Ack (returns to source)",
            )
          else span()
        },
      ),
    )
  }

  // ══════════════════════════════════════════════════════════════════════
  // Main render
  // ══════════════════════════════════════════════════════════════════════

  /** The diagram for a single member that is reporting nothing. Drawn instead of the flow diagram —
    * not a greyed-out version of it — because there is no flow to grey out: we have no snapshot from
    * this member, only the knowledge that we asked and heard nothing. Large neutral "?" marks stand
    * where the ingest and output flows would be, the persistor is drawn without figures, and the
    * cluster identity (which is known from the topology, not from this member) still anchors it.
    */
  private def renderNoDataMember(
    container: dom.HTMLElement,
    clusterInfo: Option[ClusterInfo],
    memberId: Option[Int],
  ): Unit = {
    val cx = 660.0
    val cch = 84.0 // cluster card height
    val qHalf = 60.0
    val persH = 72.0
    val clCenterY = 20.0 + cch / 2
    val cy = clCenterY + cch / 2 + 32.0 + qHalf
    val persCenterY = cy + qHalf + 32.0 + persH / 2
    val H = persCenterY + persH / 2 + 22.0
    val svg = D3
      .select(container)
      .append("svg")
      .attr("viewBox", s"0 0 $W $H")
      .attr("width", "100%")
      .attr("preserveAspectRatio", "xMidYMid meet")
      .attr("style", "display:block;font-family:Inter,system-ui,sans-serif;")
      .attr("height", H)

    // Large neutral "?" where the ingest and output flows would be — no data, no judgement — each
    // with a caption beneath it so the meaning is unambiguous.
    Seq(cx - 340.0, cx + 340.0).foreach { qx =>
      svg
        .append("text")
        .attr("x", qx)
        .attr("y", cy + 34)
        .attr("text-anchor", "middle")
        .attr("fill", C.gray)
        .attr("opacity", 0.35)
        .attr("font-size", 118)
        .attr("font-weight", 800)
        .attr("font-family", "Inter,sans-serif")
        .text("?"): Unit
      svg
        .append("text")
        .attr("x", qx)
        .attr("y", cy + 62)
        .attr("text-anchor", "middle")
        .attr("fill", C.gray)
        .attr("font-size", 13)
        .attr("font-weight", 600)
        .text("No Data Available"): Unit
    }

    // Cluster identity card — the topology is known even when this member's own state is not.
    clusterInfo.foreach { ci =>
      val ccw = 232.0; val clL = cx - ccw / 2; val clTop = clCenterY - cch / 2
      val clEl = svg.append("g")
      clEl
        .append("rect")
        .attr("x", clL)
        .attr("y", clTop)
        .attr("width", ccw)
        .attr("height", cch)
        .attr("rx", 11)
        .attr("fill", C.white)
        .attr("stroke", C.brite)
        .attr("stroke-width", 1.5)
      clEl.append("rect").attr("x", clL).attr("y", clTop).attr("width", 5).attr("height", cch).attr("fill", C.brite)
      val gx = clL + 30; val gy = clTop + cch / 2
      val mesh = Seq((gx - 12, gy + 8), (gx + 12, gy + 8), (gx, gy - 11))
      Seq((0, 1), (1, 2), (2, 0)).foreach { case (a, b) =>
        svg
          .append("line")
          .attr("x1", mesh(a)._1)
          .attr("y1", mesh(a)._2)
          .attr("x2", mesh(b)._1)
          .attr("y2", mesh(b)._2)
          .attr("stroke", C.brite)
          .attr("stroke-width", 1.4)
          .attr("opacity", 0.6): Unit
      }
      mesh.zipWithIndex.foreach { case ((px, py), idx) =>
        svg
          .append("circle")
          .attr("cx", px)
          .attr("cy", py)
          .attr("r", 4.2)
          .attr("fill", if (idx == 2) C.brite else "#dbe6f7")
          .attr("stroke", C.brite)
          .attr("stroke-width", 1.6): Unit
      }
      val ctx = clL + 60
      clEl
        .append("text")
        .attr("x", ctx)
        .attr("y", clTop + 24)
        .attr("fill", C.ink)
        .attr("font-size", 16)
        .attr("font-weight", 700)
        .text("Quine Cluster")
      clEl.append("circle").attr("cx", ctx + 3).attr("cy", clTop + 40).attr("r", 3).attr("fill", C.gray)
      clEl
        .append("text")
        .attr("x", ctx + 11)
        .attr("y", clTop + 44)
        .attr("fill", C.gray)
        .attr("font-size", 12.5)
        .attr("font-weight", 600)
        .text("No Data")
      val membersEl = clEl.append("text").attr("x", ctx).attr("y", clTop + 62)
      membersEl
        .append("tspan")
        .attr("fill", C.ink)
        .attr("font-size", 14)
        .attr("font-weight", 700)
        .text(ci.memberCount.toString)
      membersEl
        .append("tspan")
        .attr("fill", C.muted)
        .attr("font-size", 12)
        .attr("font-weight", 500)
        .text(s" Cluster Member${if (ci.memberCount == 1) "" else "s"}")
      // Hot spares only where the status endpoint reported them (see the flow-diagram cluster card).
      ci.hotSpareCount.foreach { spares =>
        val sparesEl = clEl.append("text").attr("x", ctx).attr("y", clTop + 78)
        sparesEl
          .append("tspan")
          .attr("fill", C.ink)
          .attr("font-size", 14)
          .attr("font-weight", 700)
          .text(spares.toString)
        sparesEl
          .append("tspan")
          .attr("fill", C.muted)
          .attr("font-size", 12)
          .attr("font-weight", 500)
          .text(s" Hot Spare${if (spares == 1) "" else "s"}"): Unit
      }
      (): Unit
    }

    // Q node (icon only)
    val qHalfW = 46.0
    svg
      .append("rect")
      .attr("x", cx - qHalfW)
      .attr("y", cy - qHalf)
      .attr("width", qHalfW * 2)
      .attr("height", qHalf * 2)
      .attr("rx", 22)
      .attr("fill", "#f4f6fb")
      .attr("stroke", "#bcc8e4")
      .attr("stroke-width", 1.5)
    svg
      .append("image")
      .attr("href", ServiceIcons.quineIconSvg)
      .attr("x", cx - 30)
      .attr("y", cy - 40)
      .attr("width", 60)
      .attr("height", 60)
      .attr("preserveAspectRatio", "xMidYMid meet")
    memberId.foreach { mid =>
      svg
        .append("text")
        .attr("x", cx)
        .attr("y", cy + 42)
        .attr("text-anchor", "middle")
        .attr("fill", C.ink)
        .attr("font-size", 14)
        .attr("font-weight", 700)
        .text(s"Member $mid"): Unit
    }

    // Persistor card — greyed, no figures
    val pcw = 200.0; val pcL = cx - pcw / 2; val pcTop = persCenterY - persH / 2
    val pEl = svg.append("g")
    pEl
      .append("rect")
      .attr("x", pcL)
      .attr("y", pcTop)
      .attr("width", pcw)
      .attr("height", persH)
      .attr("rx", 12)
      .attr("fill", C.white)
      .attr("stroke", C.gray)
      .attr("stroke-width", 1.5)
    pEl.append("rect").attr("x", pcL).attr("y", pcTop).attr("width", 5).attr("height", persH).attr("fill", C.gray)
    val dbx = pcL + 34; val dby = persCenterY
    pEl
      .append("ellipse")
      .attr("cx", dbx)
      .attr("cy", dby - 11)
      .attr("rx", 13)
      .attr("ry", 5)
      .attr("fill", "none")
      .attr("stroke", C.gray)
      .attr("stroke-width", 2)
    pEl
      .append("path")
      .attr(
        "d",
        s"M${dbx - 13},${dby - 11} L${dbx - 13},${dby + 11} A13,5 0 0 0 ${dbx + 13},${dby + 11} L${dbx + 13},${dby - 11}",
      )
      .attr("fill", "none")
      .attr("stroke", C.gray)
      .attr("stroke-width", 2)
    val ptx = pcL + 60
    pEl
      .append("text")
      .attr("x", ptx)
      .attr("y", pcTop + 30)
      .attr("fill", C.muted)
      .attr("font-size", 15)
      .attr("font-weight", 700)
      .text("Persistor")
    pEl.append("circle").attr("cx", ptx + 3).attr("cy", pcTop + 46).attr("r", 3).attr("fill", C.gray)
    pEl
      .append("text")
      .attr("x", ptx + 11)
      .attr("y", pcTop + 50)
      .attr("fill", C.gray)
      .attr("font-size", 12.5)
      .attr("font-weight", 600)
      .text("No Data")
    ()
  }

  private def renderDiagram(
    container: dom.HTMLElement,
    view: BackpressureView,
    clusterInfo: Option[ClusterInfo],
    nsFilter: Option[Set[String]],
  ): Unit = {
    // A single member scoped and reporting nothing gets the no-data diagram, not the flow one: the
    // view we hold is its last-known snapshot, possibly long stale, and drawing it as live flow would
    // be a fabrication. Aggregate scopes never take this path — a cluster with some members down is
    // still reporting, and says so in the status bar ("Partially Stopped").
    val noDataMemberId: Option[Int] = view.scope match {
      case BackpressureService.Scope.Member(idx) => Some(idx)
      case BackpressureService.Scope.Cluster => None
    }
    if (!view.isAggregate && view.hostsReporting.isEmpty && noDataMemberId.isDefined) {
      renderNoDataMember(container, clusterInfo, noDataMemberId)
      return
    }
    def nsVisible(ns: String): Boolean = nsFilter.forall(_.contains(ns))
    val ingests = view.ingests.filter(p => nsVisible(p.namespace)).sortBy(_.name)
    val sqs = view.standingQueries.filter(sq => nsVisible(sq.namespace)).sortBy(_.name)

    // Label things with the graph they are in only when more than one graph is on screen. A name is
    // only ambiguous against the names beside it, and in the single-graph case — which is most of
    // them, and all of OSS — the prefix would be the same word on every card. The tooltips carry the
    // graph unconditionally; this is just the at-a-glance version.
    val showGraphs = (ingests.map(_.namespace) ++ sqs.map(_.namespace)).distinct.sizeIs > 1
    val outsFlat: Seq[(StandingQueryView, SqOutputView)] =
      sqs.flatMap(sq => sq.outputs.sortBy(_.name).map(o => (sq, o)))
    val totalOuts = outsFlat.size

    // Rates arrive already differenced over the window and summed across the members in scope, so
    // these are plain field reads now rather than lookups into a side table of computed rates.
    def iRate(p: IngestView): Double = p.rate
    def oRate(o: SqOutputView): Double = o.rate
    def sqProdRate(sq: StandingQueryView): Double = sq.queue.combined.productionRate
    def sqConsRate(sq: StandingQueryView): Double = sq.queue.combined.consumptionRate

    // Where the cluster identity goes: a separate card ABOVE Q when the diagram is showing one
    // member, folded INSIDE a taller, wider Q when it is showing the whole cluster. The Q node
    // itself is then the cue for which of the two you are looking at — the distinction that most
    // changes how to read every other number on the page, carried by the shape of the thing they all
    // flow through, rather than by a label someone has to notice.
    val hasClusterCard = clusterInfo.isDefined && !view.isAggregate
    val clusterInsideQ = clusterInfo.isDefined && view.isAggregate
    // A single-member scope labels the Q with which member it is showing (drawn later); the node has
    // to grow to seat that caption, so the selection is read here too.
    val memberLabel: Option[Int] =
      if (clusterInsideQ) None
      else
        view.scope match {
          case BackpressureService.Scope.Member(idx) => Some(idx)
          case BackpressureService.Scope.Cluster => None
        }
    val rows = math.max(math.max(ingests.size, totalOuts), 2)
    // The band has to clear the ingests/outputs AND the SQ meters at their minimum gap, so an SQ's
    // name (drawn above its meter) cannot collide with its neighbour's percentage (drawn below) no
    // matter how few outputs the queries have.
    val bandHalf = math.max((rows - 1) * RowGap / 2.0, (sqs.size - 1) * SqMinGap / 2.0)
    val ingestHalf = math.max(0, (ingests.size - 1) * 13).toDouble
    val baseGHalf = math.round(math.max(56, ingestHalf + 24)).toDouble
    val gHalf =
      if (clusterInsideQ) math.max(baseGHalf, 106.0) // room for the folded-in cluster identity block
      else if (memberLabel.isDefined) math.max(baseGHalf, 74.0) // room for the "Member N" caption
      else baseGHalf
    val persCardH = 72
    val clusterH = 82.0
    val loopGap = math.max(104, bandHalf * 0.6)
    val clusterGap = math.max(86, bandHalf * 0.4)
    val aboveExtent =
      if (hasClusterCard) math.max(bandHalf + 40, gHalf + clusterGap + clusterH / 2.0 + 6)
      else math.max(bandHalf + 40, gHalf + 30)
    val belowExtent = math.max(bandHalf + 100, gHalf + loopGap + persCardH / 2.0 + 58)
    val cx = 660.0
    val cy = aboveExtent
    val H = aboveExtent + belowExtent
    val top = cy - bandHalf
    val bot = cy + bandHalf

    val open = view.globalValve.combined.isOpen
    val filteredView = view.copy(ingests = ingests, standingQueries = sqs)
    val bottleneck = computeBottleneck(filteredView)
    val (statusLabel, statusColor) = systemStatus(filteredView, bottleneck)
    // What the SVG is allowed to *point at*. Once figures have been reduced across several members
    // the bottleneck can still be computed — and system status above still reflects it, because the
    // system genuinely is backpressured — but the diagram declines to pin it to a node, since it can
    // no longer say which member's node that is. See the status bar's Bottleneck compartment.
    val shownBottleneck = if (filteredView.isAggregate) None else bottleneck
    val totalThroughput = ingests.map(iRate).sum
    // Separate rate references for ingest side and SQ output side so each scales relative to its peers
    val ingestRateRef = math.max(totalThroughput * 0.6, ingests.map(iRate).maxOption.getOrElse(1.0) * 1.2)
    // SQ production rate ref: max production rate across all SQ queues
    val sqProductionRateRef = math.max(
      sqs.map(sqProdRate).maxOption.getOrElse(1.0) * 1.2,
      100.0,
    )
    // SQ output rate ref: max per-output rate across all outputs
    val sqOutputRateRef = math.max(
      outsFlat.map { case (_, o) => oRate(o) }.maxOption.getOrElse(1.0) * 1.2,
      100.0,
    )

    val qHalfW = if (clusterInsideQ) 92.0 else 46.0
    val gLeft = cx - qHalfW; val gRight = cx + qHalfW; val gTopY = cy - gHalf; val gBotY = cy + gHalf
    val gnX = gLeft - 2 // write tab X
    val cardX = Edge.toDouble; val cardR = cardX + BoxW
    val obX = W - Edge - BoxW; val obW = BoxW
    val persCenterY = (cy + gHalf) + loopGap
    val clCenterY = (cy - gHalf) - clusterGap // cluster card above Q

    val sL = distributeYs(ingests.size, top, bot)
    val oL = distributeYs(totalOuts, top, bot)
    val nHalf = ingestHalf
    val nB = distributeYs(ingests.size, cy - nHalf, cy + nHalf)

    // Assign Y members to SQ outputs
    val outsWithY = outsFlat.zipWithIndex.map { case ((sq, o), idx) => (sq, o, oL.lift(idx).getOrElse(cy)) }
    // SQ meters are spread evenly across the band — guaranteed at least SqMinGap apart — rather than
    // centered on the mean of their own outputs' rows. Centering them packed two meters together
    // whenever their outputs happened to sit close, and their labels overlapped. The ribbons curve
    // out to reach each SQ's actual outputs, so nothing is lost by moving the meter.
    val sqYs = distributeYs(sqs.size, top, bot)
    def sqCenterY(sq: StandingQueryView): Double =
      sqs.indexWhere(s => sqKey(s) == sqKey(sq)) match {
        case -1 => cy
        case idx => sqYs.lift(idx).getOrElse(cy)
      }

    // Where each SQ's ribbon leaves the Q. Spread across the Q's right edge — mirroring the ingest
    // write-tabs on the left — so several result streams visibly fan out of the graph instead of all
    // emerging from one point near its center.
    val sqAttachHalf = math.max(0, (sqs.size - 1) * 15).toDouble
    val sqAttachYs = distributeYs(sqs.size, cy - sqAttachHalf, cy + sqAttachHalf)
    def sqAttachY(sq: StandingQueryView): Double =
      sqs.indexWhere(s => sqKey(s) == sqKey(sq)) match {
        case -1 => cy
        case idx => sqAttachYs.lift(idx).getOrElse(cy)
      }

    val svg = D3
      .select(container)
      .append("svg")
      .attr("viewBox", s"0 0 $W $H")
      .attr("width", "100%")
      .attr("preserveAspectRatio", "xMidYMid meet")
      .attr("style", "display:block;font-family:Inter,system-ui,sans-serif;")
      .attr("height", H)

    // Defs
    val defs = svg.append("defs")
    defs
      .append("linearGradient")
      .attr("id", "gFlow")
      .attr("x1", "0")
      .attr("y1", "0")
      .attr("x2", "1")
      .attr("y2", "0")
      .call { (g: js.Dynamic) =>
        g.append("stop").attr("offset", "0").attr("stop-color", C.brite);
        g.append("stop").attr("offset", "1").attr("stop-color", C.briteLite)
      }
    defs
      .append("linearGradient")
      .attr("id", "gAmber")
      .attr("x1", "0")
      .attr("y1", "0")
      .attr("x2", "1")
      .attr("y2", "0")
      .call { (g: js.Dynamic) =>
        g.append("stop").attr("offset", "0").attr("stop-color", "#b06a14");
        g.append("stop").attr("offset", "1").attr("stop-color", "#e0992f")
      }
    defs
      .append("filter")
      .attr("id", "cardSh")
      .attr("x", "-20%")
      .attr("y", "-50%")
      .attr("width", "140%")
      .attr("height", "200%")
      .append("feDropShadow")
      .attr("dx", 0)
      .attr("dy", 1)
      .attr("stdDeviation", 2.5)
      .attr("flood-color", C.ink)
      .attr("flood-opacity", 0.14)

    // Grid lines
    var gx = 0;
    while (gx <= W) {
      svg
        .append("line")
        .attr("x1", gx)
        .attr("y1", 0)
        .attr("x2", gx)
        .attr("y2", H)
        .attr("stroke", "rgba(10,41,91,0.04)")
        .attr("stroke-width", 1)
      gx += 66
    }

    // ════════ FLOW RIBBONS: Ingest → Graph Write ════════
    ingests.zipWithIndex.foreach { case (p, i) =>
      val y = sL(i); val ny = nB(i); val pRate = iRate(p); val w = thick(pRate, ingestRateRef)
      val statusTint = ingestStatusTint(p.status)
      val notFlowing = statusTint.isDefined
      val amberFlow = !notFlowing && (p.preGraphWrite.combined != PressureLevel.Flowing)
      val col = statusTint.getOrElse(if (amberFlow) "url(#gAmber)" else "url(#gFlow)")
      svg
        .append("path")
        .attr("d", ribbon(cardR, y, gnX, ny, w))
        .attr("fill", col)
        .attr("opacity", if (notFlowing) 0.18 else if (open && pRate > 1) 0.92 else 0.14)
      // Animated flowing dots on the ribbon centerline (only for actively running ingests)
      if (open && !notFlowing && pRate > 1) {
        val cl = bezierH(cardR, y, gnX, ny)
        val fast = pRate > ingestRateRef * 0.3
        val dotGap = if (fast) 30 else 48
        // White dots on thick ribbons, blue dots on thin ones for visibility
        // Use white dots on thick ribbons (visible against the dark fill),
        // blue dots on thin ribbons (visible against the light background)
        val dotColor = "#ffffff"
        val dotW = math.max(3.5, math.min(w * 0.45, 5.5))
        svg
          .append("path")
          .attr("d", cl)
          .attr("fill", "none")
          .attr("stroke", dotColor)
          .attr("stroke-width", dotW)
          .attr("stroke-linecap", "round")
          .attr("stroke-dasharray", s"0.1 $dotGap")
          .attr("class", if (fast) "flowDots" else "flowDotsSlow")
          .attr("opacity", 0.96)
      }

      // ACK return rail (below the ribbon, for checkpointed sources)
      p.postGraphWrite.foreach { s =>
        val ackBP = s.combined == PressureLevel.Backpressured
        val ac = stateColor(s.combined)
        val off = w / 2 + (if (ackBP) 13 else 8)
        val startX = gnX - 6; val endX = cardR + 12
        val dPath = edgeline(startX, ny, endX, y, off)
        svg
          .append("path")
          .attr("d", dPath)
          .attr("fill", "none")
          .attr("stroke", ac)
          .attr("stroke-width", if (ackBP) 4.5 else 1.6)
          .attr("stroke-linecap", "round")
          .attr("stroke-dasharray", if (ackBP) "7 6" else "1 7")
          .attr("opacity", if (open) (if (ackBP) 1 else 0.95) else 0.28)
          .attr("class", if (open) "ackF" else "")
        // Ack arrowhead
        svg
          .append("path")
          .attr(
            "d",
            s"M${endX + 8},${y + off - (if (ackBP) 6 else 3)} L$endX,${y + off} L${endX + 8},${y + off + (if (ackBP) 6
                                                                                                          else 3)}",
          )
          .attr("fill", "none")
          .attr("stroke", ac)
          .attr("stroke-width", if (ackBP) 3.5 else 1.6)
          .attr("stroke-linecap", "round")
          .attr("stroke-linejoin", "round")
          .attr("opacity", if (open) 0.95 else 0.4): Unit
        // Ack label
        val _t = 0.30; val _u = 1 - _t; val _dx = (endX - startX) * 0.5
        val mx =
          _u * _u * _u * startX + 3 * _u * _u * _t * (startX + _dx) + 3 * _u * _t * _t * (endX - _dx) + _t * _t * _t * endX
        val my = ny * (_u * _u * _u + 3 * _u * _u * _t) + y * (3 * _u * _t * _t + _t * _t * _t) + off
        val ackLabel = svg
          .append("text")
          .attr("x", mx)
          .attr("y", my + 4)
          .attr("text-anchor", "middle")
          .attr("fill", ac)
          .attr("font-size", 12.5)
          .attr("font-weight", 700)
          .attr("letter-spacing", "0.05em")
          .attr("opacity", if (open) 0.95 else 0.5)
          .text("ACK")
        tipOn(
          ackLabel.node().asInstanceOf[dom.Element],
          () =>
            tipHeader(s"Ack: ${p.name}") +
            tipKv(
              tipRow("post-graph-write", levelLabel(s.combined), Some(stateColor(s.combined))) +
              graphRow(p.namespace) +
              membersRow(p.members),
            ) +
            tipNote(
              if (ackBP) s"ACK BACKPRESSURE: the graph cannot acknowledge writes back to ${p.sourceType} fast enough."
              else s"After graph write commits, an ack returns to ${p.sourceType} to advance its checkpoint.",
            ),
        )
      }
    }

    // ════════ FLOW RIBBONS: Q → SQ queues → Outputs ════════
    sqs.foreach { sq =>
      val sqY = sqCenterY(sq)
      val gy = sqAttachY(sq)
      // Use client-computed production rate for the Q→queue ribbon
      val sqProdRateVal = sqProdRate(sq)
      val w = thick(sqProdRateVal, sqProductionRateRef)
      val over = sq.queue.combined.thresholdRatio >= 1.0
      val isBind = shownBottleneck.exists(bp => bp.signalType == "sq" && bp.id == sqKey(sq))
      val col = if (isBind || over) "url(#gAmber)" else "url(#gFlow)"
      val flowing = sqProdRateVal > 1
      // Q → queue
      svg
        .append("path")
        .attr("d", ribbon(gRight, gy, QueueX - 2, sqY, w))
        .attr("fill", col)
        .attr("opacity", if (flowing) 0.9 else 0.14)
      if (flowing) {
        val cl = bezierH(gRight, gy, QueueX - 2, sqY)
        val dotCol = "#ffffff"
        svg
          .append("path")
          .attr("d", cl)
          .attr("fill", "none")
          .attr("stroke", dotCol)
          .attr("stroke-width", math.max(3.5, math.min(w * 0.45, 5.5)))
          .attr("stroke-linecap", "round")
          .attr("stroke-dasharray", "0.1 30")
          .attr("class", "flowDotsSlow")
          .attr("opacity", 0.9)
      }
      // queue → each output: ribbon color from the NEXT stage downstream
      // If enrichment exists, that's the next stage; otherwise it's the destinations
      outsWithY.filter { case (s, _, _) => sqKey(s) == sqKey(sq) }.foreach { case (_, o, oy) =>
        val outRate = oRate(o)
        val dw = thick(outRate, sqOutputRateRef)
        val outFlowing = outRate > 1
        val ribbonAmber = o.enrichment match {
          case Some(e) => (e.combined != PressureLevel.Flowing) // enrichment is next stage
          case None => o.destinations.exists(_.state.combined != PressureLevel.Flowing) // destinations are next stage
        }
        svg
          .append("path")
          .attr("d", ribbon(QueueX + QmW + 2, sqY, obX, oy, dw))
          .attr("fill", if (ribbonAmber) "url(#gAmber)" else "url(#gFlow)")
          .attr("opacity", if (outFlowing) 0.9 else 0.14)
        if (outFlowing) {
          val cl = bezierH(QueueX + QmW + 2, sqY, obX, oy)
          val dotCol = "#ffffff"
          svg
            .append("path")
            .attr("d", cl)
            .attr("fill", "none")
            .attr("stroke", dotCol)
            .attr("stroke-width", math.max(3.5, math.min(dw * 0.45, 5)))
            .attr("stroke-linecap", "round")
            .attr("stroke-dasharray", "0.1 30")
            .attr("class", "flowDotsSlow")
            .attr("opacity", 0.85)
        }
      }
    }

    // ════════ GLOBAL VALVE ════════
    val vTop = top - 22; val vBot = bot + 22
    if (ingests.nonEmpty) {
      val vc = if (open) C.green else C.red
      if (open) {
        svg
          .append("line")
          .attr("x1", ValveX)
          .attr("y1", vTop)
          .attr("x2", ValveX)
          .attr("y2", vBot)
          .attr("stroke", C.green)
          .attr("stroke-width", 2.5)
          .attr("stroke-dasharray", "2 9")
          .attr("stroke-linecap", "round")
          .attr("opacity", 0.85)
      } else {
        svg
          .append("line")
          .attr("x1", ValveX)
          .attr("y1", vTop)
          .attr("x2", ValveX)
          .attr("y2", vBot)
          .attr("stroke", C.red)
          .attr("stroke-width", 5)
          .attr("stroke-linecap", "round")
          .attr("class", "valvePulse")
      }
      // Valve label box
      val vbx = ValveX - 78; val vby = vBot + 10
      val rc = view.globalValve.combined.oneMinuteClosures
      val rcColor = if (rc > 10) C.red else C.ink
      val valveBoxEl = svg.append("g")
      val valveBoxFill =
        if (!open) "rgba(224,56,77,0.1)"
        else if (rc > 10) "rgba(217,138,43,0.15)"
        else "rgba(18,165,114,0.1)"
      val valveBoxStroke = if (!open) vc else if (rc > 10) C.amber else vc
      valveBoxEl
        .append("rect")
        .attr("x", vbx)
        .attr("y", vby)
        .attr("width", 156)
        .attr("height", 46)
        .attr("rx", 8)
        .attr("fill", valveBoxFill)
        .attr("stroke", valveBoxStroke)
        .attr("stroke-width", 1)
      valveBoxEl
        .append("text")
        .attr("x", ValveX)
        .attr("y", vby + 18)
        .attr("text-anchor", "middle")
        .attr("fill", vc)
        .attr("font-size", 15)
        .attr("font-weight", 700)
        .attr("letter-spacing", "0.06em")
        .text(if (open) "VALVE OPEN" else "VALVE CLOSED")
      val valveCountText = valveBoxEl.append("text").attr("x", ValveX).attr("y", vby + 36).attr("text-anchor", "middle")
      valveCountText
        .append("tspan")
        .attr("fill", rcColor)
        .attr("font-size", 15)
        .attr("font-weight", 700)
        .text(rc.toString)
      valveCountText
        .append("tspan")
        .attr("dx", 3)
        .attr("fill", C.muted)
        .attr("font-size", 12)
        .attr("font-weight", 500)
        .text("closed / min")
      val overThresholdSqs = sqs.filter(_.queue.combined.thresholdRatio >= 1.0).map(_.name)
      val valveTipBody = () => {
        val base = tipHeader("Global ingest valve") +
          tipKv(
            tipRow("State", if (open) "Open" else "Closed", Some(vc)) +
            tipRow("Active closes", view.globalValve.combined.closedCount.toString) +
            tipRow("Closures (60s)", view.globalValve.combined.oneMinuteClosures.toString, Some(C.amber)),
          )
        val sqSection =
          if (overThresholdSqs.nonEmpty)
            tipKv(overThresholdSqs.map(n => tipRow("Queue over threshold", n, Some(C.red))).mkString)
          else ""
        val note = tipNote(
          if (open)
            "All ingests can flow. The valve closes when any SQ result queue reaches its backpressure threshold."
          else "Paused by SQ-output backpressure. All ingests are held until queues drain below threshold.",
        )
        base + sqSection + note
      }
      tipOn(valveBoxEl.node().asInstanceOf[dom.Element], valveTipBody)

      // Tooltip hover zone on the valve line itself
      val valveLineZone = svg
        .append("rect")
        .attr("x", ValveX - 10)
        .attr("y", vTop - 5)
        .attr("width", 20)
        .attr("height", vBot - vTop + 10)
        .attr("fill", "transparent")
        .attr("pointer-events", "all")
        .attr("style", "cursor:pointer;")
      tipOn(valveLineZone.node().asInstanceOf[dom.Element], valveTipBody)

      // Per-ingest dots on valve line
      ingests.zipWithIndex.foreach { case (p, i) =>
        val y = sL(i); val ny = nB(i)
        val vy = bezierYatX(cardR, y, gnX, ny, ValveX)
        val dotCol =
          ingestStatusTint(p.status).getOrElse {
            if (open) {
              if (!(p.preGraphWrite.combined != PressureLevel.Flowing)) C.brite else C.amber
            } else C.amber // valve closed — all running ingests are blocked
          }
        svg
          .append("circle")
          .attr("cx", ValveX)
          .attr("cy", vy)
          .attr("r", 5)
          .attr("fill", C.white)
          .attr("stroke", dotCol)
          .attr("stroke-width", 2)
      }
    }

    // ════════ INGEST CARDS ════════
    ingests.zipWithIndex.foreach { case (p, i) =>
      val y = sL(i); val ch = math.min(58, RowGap - 14).toDouble
      val srcState = p.source.combined
      val statusTint = ingestStatusTint(p.status)
      val notFlowing = statusTint.isDefined
      val nameTxt = ellipsize(p.name, if (showGraphs) 13 else 18)
      val pRate = iRate(p); val rt = fmtRate(pRate)
      val accentColor = statusTint.getOrElse(stateColor(srcState))
      val textColor = statusTint.getOrElse(C.ink)
      val rateColor = statusTint.getOrElse(C.brite)
      val cardEl = svg.append("g").attr("filter", "url(#cardSh)")
      cardEl
        .append("clipPath")
        // Keyed on the namespaced identity, not the bare name: an SVG id is document-scoped, and two
        // ingests of the same name in different graphs would otherwise mint the same clipPath id — the
        // second card silently clipping itself to the first card's rect.
        .attr("id", s"cl${ingestKey(p).hashCode}")
        .append("rect")
        .attr("x", cardX)
        .attr("y", y - ch / 2)
        .attr("width", BoxW)
        .attr("height", ch)
        .attr("rx", 9)
      cardEl
        .append("rect")
        .attr("x", cardX)
        .attr("y", y - ch / 2)
        .attr("width", BoxW)
        .attr("height", ch)
        .attr("rx", 9)
        .attr("fill", if (notFlowing) "#f4f5f8" else C.white)
      cardEl
        .append("rect")
        .attr("x", cardX)
        .attr("y", y - ch / 2)
        .attr("width", 5)
        .attr("height", ch)
        .attr("fill", accentColor)
        .attr("clip-path", s"url(#cl${ingestKey(p).hashCode})")
      cardEl
        .append("rect")
        .attr("x", cardX)
        .attr("y", y - ch / 2)
        .attr("width", BoxW)
        .attr("height", ch)
        .attr("rx", 9)
        .attr("fill", "none")
        .attr("stroke", statusTint.getOrElse("rgba(10,41,91,0.14)"): String)
        .attr("stroke-width", 1)
      typeIcon(cardEl, cardX + 30, y, 28, p.sourceType)
      val tcx = cardX + 60
      val nameEl = cardEl
        .append("text")
        .attr("x", tcx)
        .attr("y", y - 6)
      if (showGraphs)
        nameEl
          .append("tspan")
          .attr("fill", C.muted)
          .attr("font-size", 12)
          .attr("font-weight", 500)
          .text(s"${ellipsize(p.namespace, 9)} / "): Unit
      nameEl
        .append("tspan")
        .attr("fill", textColor)
        .attr("font-size", 17)
        .attr("font-weight", 600)
        .text(nameTxt)
      val rateTextEl = cardEl.append("text").attr("x", tcx).attr("y", y + 16)
      if (notFlowing) {
        // Not running — show the status word here in the wide text column (readable, no cramped
        // icon overlay), in place of a meaningless 0/s rate.
        val _ = rateTextEl
          .attr("fill", statusTint.getOrElse(C.gray): String)
          .attr("font-size", 15)
          .attr("font-weight", 700)
          .attr("letter-spacing", "0.03em")
          .text(p.status)
      } else {
        rateTextEl.append("tspan").attr("fill", rateColor).attr("font-size", 18).attr("font-weight", 700).text(rt)
        rateTextEl.append("tspan").attr("fill", C.muted).attr("font-size", 13).attr("font-weight", 500).text(" / s")
        p.rateLimit.foreach { limit =>
          rateTextEl
            .append("tspan")
            .attr("dx", 9)
            .attr("fill", C.muted)
            .attr("font-size", 13)
            .attr("font-weight", 500)
            .text("of ")
          rateTextEl
            .append("tspan")
            .attr("fill", C.ink)
            .attr("font-size", 14)
            .attr("font-weight", 700)
            .text(fmtRate(limit.toDouble))
          rateTextEl.append("tspan").attr("fill", C.muted).attr("font-size", 13).attr("font-weight", 500).text(" / s")
        }
      }
      // A non-running ingest isn't measuring per-stage backpressure (its stream is stopped or gated),
      // so its gauges are stale/absent — show the status in place of the stage state.
      def stageRow(label: String, raw: PressureLevel): String =
        if (notFlowing) tipRow(label, p.status, statusTint)
        else tipRow(label, levelLabel(raw), Some(stateColor(raw)))
      tipOn(
        cardEl.node().asInstanceOf[dom.Element],
        () =>
          tipHeader(s"Ingest: ${p.name}") +
          tipKv(
            tipRow("Status", p.status, statusTint) +
            graphRow(p.namespace) +
            // An ingest runs on exactly one member, so this is the whole of its attribution — there
            // is no reduction to see through, only a member to name.
            membersRow(p.members) +
            stageRow(s"Source · ${p.sourceType}", srcState) +
            stageRow("pre-graph-write", p.preGraphWrite.combined) +
            p.postGraphWrite.map(st => stageRow("post-graph-write", st.combined)).getOrElse("") +
            tipRow("Throughput", s"${fmtExact(pRate)} ev/s") +
            // Was a second row showing the server's 1-minute EWMA beside a client-computed
            // instantaneous rate. The store now differences counters over the window itself, so both
            // rows were printing the same number under two labels. Say what the one number covers.
            tipRow("Total ingested", f"${p.totalCount}%,d"),
          ) +
          tipNote(
            if (p.status == "PAUSED") "This ingest is paused. Resume it to begin processing."
            else if (p.status == "FAILED") "This ingest has failed and is no longer processing."
            else if (p.status == "RESTORED")
              "This ingest was restored from a previous session and has not been started yet."
            else if (notFlowing) s"This ingest is ${p.status.toLowerCase} and is not processing."
            else s"Ingest ${p.name}. Each state is derived from a ring buffer of 5 samples taken every 500ms.",
          ),
      )
    }

    // ════════ QUINE GRAPH NODE ════════
    val qW = gRight - gLeft; val qRX = 22
    val anyGwBp = ingests.exists(_.preGraphWrite.combined == PressureLevel.Backpressured)
    val clusterDegraded = clusterInfo.exists(_.isDegraded)
    val qPulseColor = if (clusterDegraded) C.red else if (anyGwBp) C.amber else ""
    val graphEl = svg.append("g")
    if (qPulseColor.nonEmpty)
      graphEl
        .append("rect")
        .attr("x", gLeft - 9)
        .attr("y", gTopY - 9)
        .attr("width", qW + 18)
        .attr("height", (gBotY - gTopY) + 18)
        .attr("rx", qRX + 8)
        .attr("fill", "none")
        .attr("stroke", qPulseColor)
        .attr("stroke-width", 2.5)
        .attr("class", "ringPulse")
    val qStroke = if (clusterDegraded) C.red else if (anyGwBp) C.amber else "#bcc8e4"
    val qStrokeW = if (clusterDegraded || anyGwBp) 2.5 else 1.5
    graphEl
      .append("rect")
      .attr("x", gLeft)
      .attr("y", gTopY)
      .attr("width", qW)
      .attr("height", gBotY - gTopY)
      .attr("rx", qRX)
      .attr("fill", "url(#gGlow)")
      .attr("stroke", qStroke)
      .attr("stroke-width", qStrokeW)
      .attr("filter", "url(#cardSh)")
    // The icon sits centered in the Q node, but shifts to make room for whatever is drawn beneath it:
    // up more, and larger, when the whole cluster's identity is folded in; up a little in a single-
    // member view to seat a "Member N" caption; dead-center when there is nothing below.
    val iconR = if (clusterInsideQ) 37.0 else 30.0
    val iconCy = if (clusterInsideQ) cy - 50 else if (memberLabel.isDefined) cy - 14 else cy
    graphEl
      .append("image")
      .attr("href", ServiceIcons.quineIconSvg)
      .attr("x", cx - iconR)
      .attr("y", iconCy - iconR)
      .attr("width", iconR * 2)
      .attr("height", iconR * 2)
      .attr("preserveAspectRatio", "xMidYMid meet")
    // Single-member view: say which member the diagram is scoped to, under the icon.
    memberLabel.foreach { idx =>
      graphEl
        .append("text")
        .attr("x", cx)
        .attr("y", iconCy + iconR + 20)
        .attr("text-anchor", "middle")
        .attr("fill", C.ink)
        .attr("font-size", 14)
        .attr("font-weight", 700)
        .text(s"Member $idx"): Unit
    }
    // Whole-cluster view: the cluster's identity folded into the taller, wider Q node, centered under
    // the icon so it balances with the centered logo.
    if (clusterInsideQ) clusterInfo.foreach { ci =>
      val ty = iconCy + 54
      graphEl
        .append("text")
        .attr("x", cx)
        .attr("y", ty)
        .attr("text-anchor", "middle")
        .attr("fill", C.ink)
        .attr("font-size", 21)
        .attr("font-weight", 700)
        .attr("letter-spacing", "0.01em")
        .text("Cluster")
      // Health, drawn only when it is known: an unreadable cluster status leaves the line off entirely
      // rather than asserting "Operational" about a cluster nobody could ask.
      val y1 = ty + 26
      ci.fullyUp.foreach { up =>
        val healthColor = if (up) C.green else C.red
        val healthLabel = if (up) "Operational" else "Degraded"
        val healthLn =
          graphEl.append("text").attr("x", cx).attr("y", y1).attr("text-anchor", "middle").attr("fill", healthColor)
        healthLn.append("tspan").attr("font-size", 11).text("●")
        healthLn.append("tspan").attr("font-size", 14).attr("font-weight", 600).text(s" $healthLabel"): Unit
      }
      val membersLn = graphEl.append("text").attr("x", cx).attr("y", y1 + 22).attr("text-anchor", "middle")
      membersLn
        .append("tspan")
        .attr("fill", C.ink)
        .attr("font-size", 16.5)
        .attr("font-weight", 700)
        .text(ci.memberCount.toString)
      membersLn
        .append("tspan")
        .attr("fill", C.muted)
        .attr("font-size", 14.5)
        .attr("font-weight", 500)
        .text(s" member${if (ci.memberCount == 1) "" else "s"}")
      // Likewise hot spares: only the status endpoint knows them (a spare runs nothing, so it never
      // appears in a backpressure snapshot), and "0 hot spares" is a claim, not a default.
      ci.hotSpareCount.foreach { spares =>
        val sparesLn = graphEl.append("text").attr("x", cx).attr("y", y1 + 44).attr("text-anchor", "middle")
        sparesLn
          .append("tspan")
          .attr("fill", C.ink)
          .attr("font-size", 16.5)
          .attr("font-weight", 700)
          .text(spares.toString)
        sparesLn
          .append("tspan")
          .attr("fill", C.muted)
          .attr("font-size", 14.5)
          .attr("font-weight", 500)
          .text(s" hot spare${if (spares == 1) "" else "s"}"): Unit
      }
      (): Unit
    }
    // Glow gradient
    defs
      .append("radialGradient")
      .attr("id", "gGlow")
      .call { (g: js.Dynamic) =>
        g.append("stop").attr("offset", "0").attr("stop-color", "#ffffff");
        g.append("stop").attr("offset", "1").attr("stop-color", "#e6ecf6")
      }
    tipOn(
      graphEl.node().asInstanceOf[dom.Element],
      () =>
        tipHeader("Quine graph") +
        tipKv(
          tipRow("Graph write", s"${fmtExact(totalThroughput)} ev/s") +
          tipRow("Total ingest demand", s"${fmtExact(ingests.map(iRate).sum)} ev/s"),
        ) +
        tipNote(
          "The streaming graph. Ingested events are written here via mapAsync; standing queries read matches from it.",
        ),
    )

    // ════════ PER-INGEST WRITE TABS ════════
    ingests.zipWithIndex.foreach { case (p, i) =>
      val ny = nB(i)
      val pc = stateColor(p.preGraphWrite.combined)
      val cw = 54; val chh = 22; val cl = gnX - cw; val ctop = ny - chh / 2; val tx = cl + (cw - 9) / 2
      val tabEl = svg.append("g")
      tabEl
        .append("rect")
        .attr("x", cl)
        .attr("y", ctop)
        .attr("width", cw - 9)
        .attr("height", chh)
        .attr("rx", 6)
        .attr("fill", pc)
        .attr("filter", "url(#cardSh)")
      tabEl
        .append("path")
        .attr("d", s"M${gnX - 4},${ny - 8.5} L${gnX + 8},$ny L${gnX - 4},${ny + 8.5} Z")
        .attr("fill", pc)
        .attr("stroke", C.white)
        .attr("stroke-width", 1.1)
        .attr("stroke-linejoin", "round")
      tabEl
        .append("text")
        .attr("x", tx)
        .attr("y", ny + 4)
        .attr("text-anchor", "middle")
        .attr("fill", C.white)
        .attr("font-size", 13)
        .attr("font-weight", 700)
        .attr("letter-spacing", "0.04em")
        .text("write")
      tipOn(
        tabEl.node().asInstanceOf[dom.Element],
        () => {
          val gwState = p.preGraphWrite.combined
          tipHeader(s"Graph write: ${p.name}") +
          tipKv(
            tipRow("pre-graph-write", levelLabel(gwState), Some(stateColor(gwState))) +
            graphRow(p.namespace) +
            membersRow(p.members) +
            tipRow("Write rate", s"${fmtExact(iRate(p))} ev/s"),
          ) +
          tipNote(s"Per-ingest write of ${p.name} into the Quine graph (mapAsync).")
        },
      )
    }

    // ════════ SQ RESULT QUEUES (two-phase fill) ════════
    sqs.foreach { sq =>
      val sqY = sqCenterY(sq)
      val mTop = sqY - 32; val mH = 64
      val innerH = mH - 6 // usable fill height inside the meter
      val over = sq.queue.combined.thresholdRatio >= 1.0
      val queueEl = svg.append("g")
      val sqLabelEl = queueEl
        .append("text")
        .attr("x", QueueX + QmW / 2)
        .attr("y", mTop - 9)
        .attr("text-anchor", "middle")
      if (showGraphs)
        sqLabelEl
          .append("tspan")
          .attr("fill", C.muted)
          .attr("font-size", 11.5)
          .attr("font-weight", 500)
          .text(s"${ellipsize(sq.namespace, 9)} / "): Unit
      sqLabelEl
        .append("tspan")
        .attr("fill", C.ink)
        .attr("font-size", 15.5)
        .attr("font-weight", 700)
        .text(ellipsize(sq.name, if (showGraphs) 14 else 20))
      // Meter background
      queueEl
        .append("rect")
        .attr("x", QueueX)
        .attr("y", mTop)
        .attr("width", QmW)
        .attr("height", mH)
        .attr("rx", 7)
        .attr("fill", C.track)
        .attr("stroke", if (over) C.red else "rgba(10,41,91,0.2)")
        .attr("stroke-width", if (over) 1.8 else 1.2)
        .attr("filter", "url(#cardSh)")

      {
        val qi = sq.queue.combined
        if (qi.thresholdRatio < 1.0) {
          // Phase 1: filling toward threshold. Color blends from blue → amber as it fills.
          val ratio = math.max(qi.thresholdRatio, 0.0)
          val fillH = ratio * innerH
          // Interpolate color: brite (#1658b7) at 0% → amber (#d98a2b) at 100%
          val fillColor = if (ratio < 0.3) C.brite else if (ratio < 0.7) C.amber else C.amber
          if (fillH > 1)
            queueEl
              .append("rect")
              .attr("x", QueueX + 3)
              .attr("y", mTop + mH - 3 - fillH)
              .attr("width", QmW - 6)
              .attr("height", fillH)
              .attr("rx", 4)
              .attr("fill", fillColor)
          val pct = math.round(ratio * 100).toInt
          val pctColor = if (ratio < 0.3) C.brite else C.amber
          queueEl
            .append("text")
            .attr("x", QueueX + QmW / 2)
            .attr("y", mTop + mH + 15)
            .attr("text-anchor", "middle")
            .attr("fill", pctColor)
            .attr("font-size", 15)
            .attr("font-weight", 700)
            .text(s"$pct%")
        } else {
          // Phase 2: past threshold, filling toward max capacity. Turns red.
          val ratio = math.max(qi.capacityRatio, 0.0)
          val fillH = math.max(3, ratio * innerH)
          queueEl
            .append("rect")
            .attr("x", QueueX + 3)
            .attr("y", mTop + mH - 3 - fillH)
            .attr("width", QmW - 6)
            .attr("height", fillH)
            .attr("rx", 4)
            .attr("fill", C.red)
          val capPct = math.round(ratio * 100).toInt
          queueEl
            .append("text")
            .attr("x", QueueX + QmW / 2)
            .attr("y", mTop + mH + 15)
            .attr("text-anchor", "middle")
            .attr("fill", C.red)
            .attr("font-size", 15)
            .attr("font-weight", 700)
            .text(s"$capPct%")
        }
      }
      // Threshold tick line inside the meter
      val threshLineY = mTop + 3
      queueEl
        .append("line")
        .attr("x1", QueueX + 2)
        .attr("y1", threshLineY)
        .attr("x2", QueueX + QmW - 2)
        .attr("y2", threshLineY)
        .attr("stroke", C.ink)
        .attr("stroke-width", 1.5)
        .attr("opacity", 0.35)
      val pct = math.round(sq.queue.combined.thresholdRatio * 100).toInt
      val qc = if (over) C.red else if (pct >= 50) C.amber else C.brite
      tipOn(
        queueEl.node().asInstanceOf[dom.Element],
        () => {
          val qi = sq.queue.combined
          // A colspan section header (valid inside the kv <table>). Shown only when aggregating, to
          // make visible what the prose otherwise has to explain: the fill figures are the single
          // worst member's, while the rates and totals below are summed across the cluster.
          def sect(t: String): String =
            if (!view.isAggregate) ""
            else
              s"""<tr><td colspan="2" style="padding:7px 0 1px 0;font-size:10px;letter-spacing:0.08em;""" +
              s"""text-transform:uppercase;color:#8b93b0;">${LandingTooltip.escape(t)}</td></tr>"""
          tipHeader(s"SQ result queue: ${sq.name}") +
          tipKv(
            queueRow("State", sq.queue) +
            graphRow(sq.namespace) +
            sect("Worst-case member") +
            tipRow("Buffered", f"${qi.bufferCount}%,d") +
            tipRow("Threshold", qi.backpressureThreshold.toString, if (over) Some(C.red) else None) +
            tipRow("Max capacity", f"${qi.maxSize}%,d") +
            tipRow("Threshold fill", s"$pct%", Some(qc)) +
            sect("Cluster totals (Σ)") +
            tipRow("Production rate", s"${fmtExact(sqProdRate(sq))} / s") +
            tipRow("Consumption rate", s"${fmtExact(sqConsRate(sq))} / s") +
            tipRow("Total produced", f"${qi.totalProduced}%,d") +
            tipRow("Total cancellations", f"${qi.totalCancellations}%,d") +
            tipRow("Total dropped", f"${qi.totalDropped}%,d", if (qi.totalDropped > 0) Some(C.red) else None) +
            tipRow("Outputs", sq.outputs.size.toString),
          ) +
          tipNote(
            if (over)
              s"Queue has exceeded the backpressure threshold. The global ingest valve is closed until it drains."
            else s"$pct% of the way to the backpressure threshold.",
          )
        },
      )
    }

    // ════════ OUTPUT CARDS ════════
    outsWithY.foreach { case (sq, o, y) =>
      val ch = math.min(58, RowGap - 14).toDouble
      val outputName = o.name
      val nameTxt = ellipsize(outputName, 18)
      // Extract per-destination types and their states. `state` is the worst across the members
      // in scope — a destination backpressured on any one member colors the card, since that is
      // exactly the single saturated member the diagram exists to surface.
      val destSlugs = o.destinations.map(d => (d.`type`, d.state.combined))
      val worstDestState =
        o.destinations.map(_.state.combined).maxOption.getOrElse(PressureLevel.Flowing)
      val wc = stateColor(worstDestState)
      val outRateVal = oRate(o); val rt = fmtRate(outRateVal)
      val cardEl = svg.append("g").attr("filter", "url(#cardSh)")
      // Likewise namespaced — and qualified by the standing query, since two queries may each have an
      // output called "kafka-out" even within one graph.
      val cid = s"clo${outputKey(sq, o).hashCode}"
      cardEl
        .append("clipPath")
        .attr("id", cid)
        .append("rect")
        .attr("x", obX)
        .attr("y", y - ch / 2)
        .attr("width", obW)
        .attr("height", ch)
        .attr("rx", 9)
      cardEl
        .append("rect")
        .attr("x", obX)
        .attr("y", y - ch / 2)
        .attr("width", obW)
        .attr("height", ch)
        .attr("rx", 9)
        .attr("fill", C.white)
      cardEl
        .append("rect")
        .attr("x", obX + obW - 5)
        .attr("y", y - ch / 2)
        .attr("width", 5)
        .attr("height", ch)
        .attr("fill", wc)
        .attr("clip-path", s"url(#$cid)")
      cardEl
        .append("rect")
        .attr("x", obX)
        .attr("y", y - ch / 2)
        .attr("width", obW)
        .attr("height", ch)
        .attr("rx", 9)
        .attr("fill", "none")
        .attr("stroke", "rgba(10,41,91,0.14)")
        .attr("stroke-width", 1)

      // Stacked destination icons — backpressured one comes to front
      // Shift icon base left when multiple destinations so stack doesn't overlap accent stripe
      val iconBaseX = obX + obW - 30.0 - (math.min(destSlugs.size, 3) - 1) * 5.0
      val iconSz = 28.0
      if (destSlugs.size <= 1) {
        // Single destination: icon with state-colored border
        val singleState = destSlugs.headOption.map(_._2).getOrElse(PressureLevel.Flowing)
        val singleSlug = destSlugs.headOption.map(_._1).getOrElse("unknown")
        val borderColor = stateColor(singleState)
        val r = iconSz / 2
        cardEl
          .append("rect")
          .attr("x", iconBaseX - r - 4)
          .attr("y", y - r - 4)
          .attr("width", iconSz + 8)
          .attr("height", iconSz + 8)
          .attr("rx", 7)
          .attr("fill", "#eef1f7")
          .attr("stroke", borderColor)
          .attr("stroke-width", 1.8)
        ServiceIcons.forType(singleSlug) match {
          case Some(iconUrl) =>
            cardEl
              .append("image")
              .attr("href", iconUrl)
              .attr("x", iconBaseX - r)
              .attr("y", y - r)
              .attr("width", iconSz)
              .attr("height", iconSz)
              .attr("preserveAspectRatio", "xMidYMid meet")
          case None =>
            typeIcon(cardEl, iconBaseX, y, iconSz, singleSlug)
        }
      } else {
        // Multiple destinations: smaller stacked icons, backpressured one on top
        val smallSz = 22 // smaller than single-destination's 28
        val stackOffset = 4 // px offset per stacked icon
        val sorted = destSlugs.sortBy { case (_, state) => state.severity }
        val capped = sorted.take(3)
        val yShift = (capped.size - 1) * stackOffset * 0.35
        capped.zipWithIndex.foreach { case ((slug, state), idx) =>
          val ix = iconBaseX + idx * stackOffset
          val iy = y + yShift - idx * stackOffset
          val borderColor = stateColor(state)
          val r = smallSz / 2
          cardEl
            .append("rect")
            .attr("x", ix - r - 3)
            .attr("y", iy - r - 3)
            .attr("width", smallSz + 6)
            .attr("height", smallSz + 6)
            .attr("rx", 6)
            .attr("fill", "#eef1f7")
            .attr("stroke", borderColor)
            .attr("stroke-width", 1.6)
          ServiceIcons.forType(slug) match {
            case Some(iconUrl) =>
              cardEl
                .append("image")
                .attr("href", iconUrl)
                .attr("x", ix - r)
                .attr("y", iy - r)
                .attr("width", smallSz)
                .attr("height", smallSz)
                .attr("preserveAspectRatio", "xMidYMid meet")
            case None =>
              val mono = slug match {
                case t if t.contains("standard-out") || t.contains("stdout") => ">_"
                case t if t.contains("drop") => "∅"
                case t if t.contains("log") => "≡"
                case t => t.take(2)
              }
              cardEl
                .append("text")
                .attr("x", ix)
                .attr("y", iy + smallSz * 0.19)
                .attr("text-anchor", "middle")
                .attr("fill", C.ink)
                .attr("font-size", smallSz * 0.46)
                .attr("font-weight", 800)
                .attr("font-family", "Inter,sans-serif")
                .text(mono)
          }
        }
      }

      // Enrichment indicator — left accent bar + Q icon at card bottom-left
      // Sticky: once seen as enriched, stays enriched (config doesn't change at runtime)
      val enrichKey = outputKey(sq, o)
      val preWfState = o.enrichment.map(_.combined).getOrElse(PressureLevel.Flowing)
      if (o.hasEnrichment) enrichmentCache += enrichKey
      val showEnrichment = enrichmentCache.contains(enrichKey)
      if (showEnrichment) {
        val enrichColor = stateColor(preWfState)
        // Left accent bar (mirrors the right-side destination accent bar)
        cardEl
          .append("rect")
          .attr("x", obX)
          .attr("y", y - ch / 2)
          .attr("width", 5)
          .attr("height", ch)
          .attr("fill", enrichColor)
          .attr("clip-path", s"url(#$cid)")
        // Q icon at bottom-left of card
        val qSz = 22.0; val qX = obX + 20; val qY = y + ch / 2 - 14
        cardEl
          .append("image")
          .attr("href", ServiceIcons.quineIconSvg)
          .attr("x", qX - qSz / 2)
          .attr("y", qY - qSz / 2)
          .attr("width", qSz)
          .attr("height", qSz)
          .attr("preserveAspectRatio", "xMidYMid meet"): Unit
        // Tooltip hover zone over the enrichment bar + icon area
        val enrichEl = cardEl
          .append("rect")
          .attr("x", obX)
          .attr("y", y - ch / 2)
          .attr("width", 28)
          .attr("height", ch)
          .attr("fill", "transparent")
          .attr("pointer-events", "all")
          .attr("style", "cursor:pointer;")
        tipOn(
          enrichEl.node().asInstanceOf[dom.Element],
          () =>
            tipHeader(s"Enrichment: $outputName") +
            tipKv(
              (o.enrichment match {
                case Some(e) => levelRow("Enrichment stage", e)
                case None => tipRow("Enrichment stage", levelLabel(preWfState), Some(stateColor(preWfState)))
              }) +
              graphRow(sq.namespace) +
              tipRow("Throughput", s"${fmtExact(outRateVal)} / s") +
              tipRow("Total processed", f"${o.totalCount}%,d"),
            ) +
            tipNote(
              "Results pass through an enrichment query (resultEnrichment or preEnrichmentTransformation) before reaching destinations. " +
              "Backpressure here means the enrichment Cypher query cannot keep up.",
            ),
        )
      }

      val tEnd = obX + obW - 66
      cardEl
        .append("text")
        .attr("x", tEnd)
        .attr("y", y - 6)
        .attr("text-anchor", "end")
        .attr("fill", C.ink)
        .attr("font-size", 17)
        .attr("font-weight", 600)
        .text(nameTxt)
      val rateEl = cardEl.append("text").attr("x", tEnd).attr("y", y + 16).attr("text-anchor", "end")
      rateEl.append("tspan").attr("fill", C.brite).attr("font-size", 18).attr("font-weight", 700).text(rt)
      rateEl.append("tspan").attr("fill", C.muted).attr("font-size", 13).attr("font-weight", 500).text(" / s")
      val isGating = o.destinations.exists(_.state.combined == PressureLevel.Backpressured)
      val isEnrichmentBottleneck = preWfState == PressureLevel.Backpressured

      tipOn(
        cardEl.node().asInstanceOf[dom.Element],
        () =>
          tipHeader(s"Output: ${sq.name}/$outputName") +
          tipKv(
            (o.enrichment match {
              case Some(e) if showEnrichment => levelRow("enrichment", e)
              case _ => ""
            }) +
            graphRow(sq.namespace) +
            o.destinations.map(d => levelRow(d.`type`, d.state)).mkString +
            tipRow("Throughput", s"${fmtExact(outRateVal)} / s") +
            tipRow("Total processed", f"${o.totalCount}%,d") +
            (if (isGating) tipRow("Gating", "yes, slow destination", Some(C.amber))
             else if (isEnrichmentBottleneck) tipRow("Gating", "yes, slow enrichment query", Some(C.amber))
             else ""),
          ) +
          tipNote(
            if (isEnrichmentBottleneck)
              s"The enrichment query on this output cannot keep up. It is gating the queue drain."
            else if (isGating) s"A destination on this output cannot keep up. It is gating the queue drain."
            else
              s"Consumes matches from the ${sq.name} result queue.${if (showEnrichment) " Has enrichment query."
              else ""} ${o.destinations.size} destination(s).",
          ),
      )
    }

    // ════════ PERSISTOR CARD ════════
    {
      val pcw = 200; val pch = persCardH; val cardT = persCenterY - pch / 2; val cardL = cx - pcw / 2
      val acc = C.brite
      val dnX = cx - 17; val upX = cx + 17; val pTopY = cardT
      // Persist/restore dashed arrows
      svg
        .append("path")
        .attr("d", bezierH(dnX, gBotY, dnX, pTopY - 2))
        .attr("fill", "none")
        .attr("stroke", acc)
        .attr("stroke-width", 3)
        .attr("stroke-linecap", "round")
        .attr("stroke-dasharray", "1 6")
        .attr("opacity", if (open) 0.85 else 0.25)
        .attr("class", if (open) "ackF" else "")
      svg
        .append("path")
        .attr("d", bezierH(upX, pTopY - 2, upX, gBotY))
        .attr("fill", "none")
        .attr("stroke", acc)
        .attr("stroke-width", 3)
        .attr("stroke-linecap", "round")
        .attr("stroke-dasharray", "1 6")
        .attr("opacity", if (open) 0.85 else 0.25)
        .attr("class", if (open) "ackF" else "")
      // Arrowheads
      svg
        .append("path")
        .attr("d", s"M${dnX - 4},${pTopY - 7} L$dnX,${pTopY - 1} L${dnX + 4},${pTopY - 7}")
        .attr("fill", "none")
        .attr("stroke", acc)
        .attr("stroke-width", 2.4)
        .attr("stroke-linecap", "round")
        .attr("stroke-linejoin", "round")
      svg
        .append("path")
        .attr("d", s"M${upX - 4},${gBotY + 5} L$upX,${gBotY - 1} L${upX + 4},${gBotY + 5}")
        .attr("fill", "none")
        .attr("stroke", acc)
        .attr("stroke-width", 2.4)
        .attr("stroke-linecap", "round")
        .attr("stroke-linejoin", "round")
      // Card
      val chipS = 48; val chipX = cardL + 8; val chipY = cardT + (pch - chipS) / 2
      val persEl = svg.append("g")
      val pcId = s"clp${view.persistor.combined.`type`.hashCode}"
      persEl
        .append("clipPath")
        .attr("id", pcId)
        .append("rect")
        .attr("x", cardL)
        .attr("y", cardT)
        .attr("width", pcw)
        .attr("height", pch)
        .attr("rx", 11)
      persEl
        .append("rect")
        .attr("x", cardL)
        .attr("y", cardT)
        .attr("width", pcw)
        .attr("height", pch)
        .attr("rx", 11)
        .attr("fill", C.white)
        .attr("filter", "url(#cardSh)")
      persEl
        .append("rect")
        .attr("x", cardL)
        .attr("y", cardT)
        .attr("width", 5)
        .attr("height", pch)
        .attr("fill", acc)
        .attr("clip-path", s"url(#$pcId)")
      persEl
        .append("rect")
        .attr("x", cardL)
        .attr("y", cardT)
        .attr("width", pcw)
        .attr("height", pch)
        .attr("rx", 11)
        .attr("fill", "none")
        .attr("stroke", acc)
        .attr("stroke-width", 1.5)
      // Use real persistor icon from ServiceIcons when available — no background box when icon exists
      ServiceIcons.forType(view.persistor.combined.`type`) match {
        case Some(iconUrl) =>
          persEl
            .append("image")
            .attr("href", iconUrl)
            .attr("x", chipX)
            .attr("y", chipY)
            .attr("width", chipS)
            .attr("height", chipS)
            .attr("preserveAspectRatio", "xMidYMid meet")
        case None =>
          persEl
            .append("rect")
            .attr("x", chipX)
            .attr("y", chipY)
            .attr("width", chipS)
            .attr("height", chipS)
            .attr("rx", 9)
            .attr("fill", "#cfe0f7")
          val mono = view.persistor.combined.`type` match {
            case t if t.contains("rocks") => "R"
            case t if t.contains("cassandra") => "C"
            case t if t.contains("mapdb") => "M"
            case t if t.contains("click") => "CH"
            case t if t.contains("postgres") => "Pg"
            case t if t.contains("memory") || t.contains("mem") => "∞"
            case t => t.take(2).capitalize
          }
          persEl
            .append("text")
            .attr("x", chipX + chipS / 2)
            .attr("y", chipY + chipS / 2 + 6)
            .attr("text-anchor", "middle")
            .attr("fill", acc)
            .attr("font-size", 21)
            .attr("font-weight", 800)
            .text(mono)
      }
      val tx = chipX + chipS + 8
      persEl
        .append("text")
        .attr("x", tx)
        .attr("y", cardT + 27)
        .attr("fill", acc)
        .attr("font-size", 18.5)
        .attr("font-weight", 700)
        .text(view.persistor.combined.`type`)
      persEl.append("circle").attr("cx", tx + 4).attr("cy", cardT + 41).attr("r", 3.5).attr("fill", C.green)
      persEl
        .append("text")
        .attr("x", tx + 13)
        .attr("y", cardT + 45)
        .attr("fill", C.muted)
        .attr("font-size", 12.5)
        .attr("font-weight", 600)
        .attr("letter-spacing", "0.05em")
        .text("PERSISTOR")
      val latEl = persEl.append("text").attr("x", tx).attr("y", cardT + 61)
      latEl.append("tspan").attr("fill", C.muted).attr("font-size", 12.5).attr("font-weight", 500).text("w ")
      latEl
        .append("tspan")
        .attr("fill", C.ink)
        .attr("font-size", 13.5)
        .attr("font-weight", 700)
        .text(f"${view.persistor.combined.writeLatencyMs}%.2f")
      latEl.append("tspan").attr("fill", C.muted).attr("font-size", 12.5).attr("font-weight", 500).text(" · r ")
      latEl
        .append("tspan")
        .attr("fill", C.ink)
        .attr("font-size", 13.5)
        .attr("font-weight", 700)
        .text(f"${view.persistor.combined.readLatencyMs}%.2f")
      latEl
        .append("tspan")
        .attr("dx", 2)
        .attr("fill", C.muted)
        .attr("font-size", 12.5)
        .attr("font-weight", 500)
        .text("ms")
      tipOn(
        persEl.node().asInstanceOf[dom.Element],
        () =>
          tipHeader(s"Persistor: ${view.persistor.combined.`type`}") +
          tipKv(
            tipRow("Store type", view.persistor.combined.`type`) +
            tipRow("Write latency", f"${view.persistor.combined.writeLatencyMs}%.2f ms") +
            tipRow("Read latency", f"${view.persistor.combined.readLatencyMs}%.2f ms"),
          ) +
          tipNote("Quine writes every graph mutation here for durability and reads node state back on demand."),
      )
    }

    // ════════ CLUSTER CARD (above Q — only when the diagram is scoped to a single member) ════════
    // When the whole cluster is in view, this identity lives inside the Q node instead, and drawing
    // both would say the same thing twice.
    if (hasClusterCard) clusterInfo.foreach { ci =>
      val clAccNormal = C.brite; val clLite = "#dbe6f7"
      val clAcc = if (ci.isDegraded) C.red else clAccNormal
      val ccw = 220.0; val cch = clusterH; val clTop = clCenterY - cch / 2; val clBot = clCenterY + cch / 2;
      val clL = cx - ccw / 2
      val dn2 = cx - 17; val up2 = cx + 17

      // Bidirectional dashed sync arrows Q ↔ cluster
      svg
        .append("path")
        .attr("d", bezierH(dn2, clBot, dn2, gTopY + 2))
        .attr("fill", "none")
        .attr("stroke", clAcc)
        .attr("stroke-width", 3)
        .attr("stroke-linecap", "round")
        .attr("stroke-dasharray", "1 6")
        .attr("opacity", 0.8)
        .attr("class", "ackF")
      svg
        .append("path")
        .attr("d", bezierH(up2, gTopY + 2, up2, clBot))
        .attr("fill", "none")
        .attr("stroke", clAcc)
        .attr("stroke-width", 3)
        .attr("stroke-linecap", "round")
        .attr("stroke-dasharray", "1 6")
        .attr("opacity", 0.8)
        .attr("class", "ackF")
      // Arrowheads
      svg
        .append("path")
        .attr("d", s"M${dn2 - 4},${gTopY - 5} L$dn2,${gTopY + 1} L${dn2 + 4},${gTopY - 5}")
        .attr("fill", "none")
        .attr("stroke", clAcc)
        .attr("stroke-width", 2.4)
        .attr("stroke-linecap", "round")
        .attr("stroke-linejoin", "round")
      svg
        .append("path")
        .attr("d", s"M${up2 - 4},${clBot + 7} L$up2,${clBot + 1} L${up2 + 4},${clBot + 7}")
        .attr("fill", "none")
        .attr("stroke", clAcc)
        .attr("stroke-width", 2.4)
        .attr("stroke-linecap", "round")
        .attr("stroke-linejoin", "round")

      // Cluster card
      val clEl = svg.append("g")
      val clId = "clc"
      clEl
        .append("clipPath")
        .attr("id", clId)
        .append("rect")
        .attr("x", clL)
        .attr("y", clTop)
        .attr("width", ccw)
        .attr("height", cch)
        .attr("rx", 11)
      clEl
        .append("rect")
        .attr("x", clL)
        .attr("y", clTop)
        .attr("width", ccw)
        .attr("height", cch)
        .attr("rx", 11)
        .attr("fill", C.white)
        .attr("filter", "url(#cardSh)")
      clEl
        .append("rect")
        .attr("x", clL)
        .attr("y", clTop)
        .attr("width", 5)
        .attr("height", cch)
        .attr("fill", clAcc)
        .attr("clip-path", s"url(#$clId)")
      clEl
        .append("rect")
        .attr("x", clL)
        .attr("y", clTop)
        .attr("width", ccw)
        .attr("height", cch)
        .attr("rx", 11)
        .attr("fill", "none")
        .attr("stroke", clAcc)
        .attr("stroke-width", 1.5)

      // Peer mesh icon (triangle of 3 nodes)
      val gx = clL + 29; val gy = clTop + cch / 2
      val mesh = Seq((gx - 12, gy + 8), (gx + 12, gy + 8), (gx, gy - 11))
      // Draw triangle edges: 0→1, 1→2, 2→0
      val edges = Seq((0, 1), (1, 2), (2, 0))
      edges.foreach { case (a, b) =>
        svg
          .append("line")
          .attr("x1", mesh(a)._1)
          .attr("y1", mesh(a)._2)
          .attr("x2", mesh(b)._1)
          .attr("y2", mesh(b)._2)
          .attr("stroke", clAcc)
          .attr("stroke-width", 1.4)
          .attr("opacity", 0.6)
      }
      mesh.zipWithIndex.foreach { case ((px, py), idx) =>
        svg
          .append("circle")
          .attr("cx", px)
          .attr("cy", py)
          .attr("r", 4.2)
          .attr("fill", if (idx == 2) clAcc else clLite)
          .attr("stroke", clAcc)
          .attr("stroke-width", 1.6)
      }

      // Text labels
      val ctx = clL + 59
      // Line 1: Title — branded "Quine" wordmark SVG + "Cluster" text
      val wmH = 16.0; val wmW = wmH * (2180.0 / 642.0) // match viewBox aspect ratio
      clEl
        .append("image")
        .attr("href", ServiceIcons.quineWordmark)
        .attr("x", ctx)
        .attr("y", clTop + 8)
        .attr("width", wmW)
        .attr("height", wmH)
        .attr("preserveAspectRatio", "xMinYMid meet"): Unit
      clEl
        .append("text")
        .attr("x", ctx + wmW + 5)
        .attr("y", clTop + 22)
        .attr("fill", clAccNormal)
        .attr("font-size", 17.5)
        .attr("font-weight", 700)
        .attr("font-family", "Inter")
        .text("Cluster")
      // Line 2: Status dot + Operational/Degraded — only when the cluster status is readable.
      val healthColor = ci.fullyUp.map(up => if (up) C.green else C.red).getOrElse(C.muted)
      ci.fullyUp.foreach { up =>
        val healthLabel = if (up) "Operational" else "Degraded"
        clEl.append("circle").attr("cx", ctx + 3).attr("cy", clTop + 36).attr("r", 3).attr("fill", healthColor)
        clEl
          .append("text")
          .attr("x", ctx + 11)
          .attr("y", clTop + 40)
          .attr("fill", healthColor)
          .attr("font-size", 12.5)
          .attr("font-weight", 600)
          .text(healthLabel): Unit
      }
      // Line 3: Member count
      val membersEl = clEl.append("text").attr("x", ctx).attr("y", clTop + 56)
      membersEl
        .append("tspan")
        .attr("fill", C.ink)
        .attr("font-size", 15)
        .attr("font-weight", 700)
        .text(ci.memberCount.toString)
      membersEl
        .append("tspan")
        .attr("fill", C.muted)
        .attr("font-size", 13)
        .attr("font-weight", 500)
        .text(s" Cluster Member${if (ci.memberCount == 1) "" else "s"}")
      // Line 4: Hot spares — likewise only where they are knowable.
      ci.hotSpareCount.foreach { spares =>
        val sparesEl = clEl.append("text").attr("x", ctx).attr("y", clTop + 72)
        sparesEl
          .append("tspan")
          .attr("fill", C.ink)
          .attr("font-size", 15)
          .attr("font-weight", 700)
          .text(spares.toString)
        sparesEl
          .append("tspan")
          .attr("fill", C.muted)
          .attr("font-size", 13)
          .attr("font-weight", 500)
          .text(s" Hot Spare${if (spares == 1) "" else "s"}"): Unit
      }

      tipOn(
        clEl.node().asInstanceOf[dom.Element],
        () =>
          tipHeader("Quine cluster") +
          tipKv(
            ci.fullyUp.fold("")(up => tipRow("Status", if (up) "Operational" else "Degraded", Some(healthColor))) +
            tipRow("Cluster members", ci.memberCount.toString) +
            ci.hotSpareCount.fold("")(spares => tipRow("Hot spares", spares.toString)),
          ) +
          tipNote(
            "This node is one member of a Quine cluster. Each peer runs the same pipeline over its own shard and exchanges graph state bidirectionally.",
          ),
      )
    }

    // ════════ BOTTLENECK TAG ════════
    // Position the pill on the specific node that is the bottleneck
    shownBottleneck.foreach { bp =>
      val (tagX, tagY) = bp.signalType match {
        case "gw" =>
          // On the Q node — positioned at the top edge
          (cx, gTopY - 13)
        case "ack" | "ingest" =>
          // On the ingest card
          val idx = ingests.indexWhere(p => ingestKey(p) == bp.id)
          val yy = sL.lift(idx).getOrElse(cy)
          val ch = math.min(58, RowGap - 14).toDouble
          (cardX + BoxW / 2.0, yy - ch / 2 - 13)
        case "sq" =>
          // On the SQ queue
          val sqY0 = sqs.find(s => sqKey(s) == bp.id).map(sqCenterY).getOrElse(cy)
          (QueueX + QmW / 2.0, sqY0 - 64)
        case "sq-output" | "sq-enrichment" =>
          // On the output card — bp.id is this output's namespaced key
          val outIdx = outsFlat.indexWhere { case (sq, o) => outputKey(sq, o) == bp.id }
          val yy = oL.lift(outIdx).getOrElse(cy)
          val ch = math.min(58, RowGap - 14).toDouble
          val hasEnrich = enrichmentCache.contains(bp.id)
          val xPos =
            if (!hasEnrich) obX + obW / 2.0 // centered (no enrichment)
            else if (bp.signalType == "sq-enrichment") obX + obW * 0.25 // left-aligned (enrichment bottleneck)
            else obX + obW * 0.75 // right-aligned (destination bottleneck)
          (xPos, yy - ch / 2 - 13)
        case _ =>
          (cx, gTopY - 30)
      }
      val clampedY = math.max(16.0, tagY)
      svg
        .append("rect")
        .attr("x", tagX - 66)
        .attr("y", clampedY - 11)
        .attr("width", 132)
        .attr("height", 21)
        .attr("rx", 10.5)
        .attr("fill", C.amber)
      svg
        .append("text")
        .attr("x", tagX)
        .attr("y", clampedY + 3.5)
        .attr("text-anchor", "middle")
        .attr("fill", C.white)
        .attr("font-size", 13)
        .attr("font-weight", 700)
        .attr("letter-spacing", "0.08em")
        .text("BOTTLENECK")
    }

    // ════════ STAGE AXIS ════════
    val axisY = H - 6
    val axis = Seq(
      ("Ingest Source", cardX + BoxW / 2),
      ("Global Valve", ValveX.toDouble),
      ("Graph & Persistor", cx),
      ("Standing Query Results", QueueX + QmW / 2.0),
      ("Standing Query Outputs", obX + obW / 2.0),
    )
    axis.foreach { case (t, x) =>
      svg
        .append("text")
        .attr("x", x)
        .attr("y", axisY)
        .attr("text-anchor", "middle")
        .attr("fill", C.gray)
        .attr("font-size", 14.5)
        .attr("font-family", "'Roboto Slab',serif")
        .attr("letter-spacing", "0.06em")
        .text(t)
    }

    // ════════ CSS ANIMATIONS ════════
    (svg
      .append("style")
      .text("""@keyframes flowF { to { stroke-dashoffset:-240; } }
        |@keyframes flowSlow { to { stroke-dashoffset:-240; } }
        |@keyframes ackF { to { stroke-dashoffset:-96; } }
        |@keyframes ringPulse { 0%,100%{opacity:.55;} 50%{opacity:.08;} }
        |@keyframes valvePulse { 0%,100%{opacity:1;} 50%{opacity:.45;} }
        |.flowDots { animation:flowF 1.4s linear infinite; }
        |.flowDotsSlow { animation:flowSlow 2.2s linear infinite; }
        |.ringPulse{ animation:ringPulse 1.8s ease-in-out infinite; }
        |.valvePulse{ animation:valvePulse 1.1s ease-in-out infinite; }
        |.ackF{ animation:ackF 1.6s linear infinite; }
        |""".stripMargin)): Unit
  }
}
