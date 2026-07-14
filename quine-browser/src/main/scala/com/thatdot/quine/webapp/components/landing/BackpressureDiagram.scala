package com.thatdot.quine.webapp.components.landing

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.webapp.components.D3
import com.thatdot.quine.webapp.components.landing.RateComputer.EnrichedSnapshot
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

  private def stateColor(state: String): String = state.toUpperCase match {
    case "FLOWING" => C.green
    case "CONSTRAINED" => C.yellow
    case "BACKPRESSURED" => C.amber
    case _ => C.gray
  }

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
  private val BoxW = 234.0
  private val Edge = 20.0
  private val QmW = 26.0 // queue meter width
  private val ValveX = 400.0
  private val QueueX = W - ValveX - QmW // ≈894, mirrored

  // ── Cluster info for diagram rendering ──
  private case class ClusterInfo(memberCount: Int, hotSpareCount: Int, fullyUp: Boolean)

  // ── Derived state ──
  // Bottleneck signal: type (for pill positioning), label, state, rank
  private case class BpSignal(signalType: String, id: String, label: String, state: String, rank: Int)

  /** Compute the bottleneck:
    * - Collect all signals with their downstream rank:
    *   SQ output destinations (rank 6) > enrichment (5) > SQ queue (4) > graph-write/ack (3)
    * - Filter to only "BACKPRESSURED" signals (CONSTRAINED is informational, never a bottleneck)
    * - The highest-rank backpressured signal is the bottleneck
    * - When valve is closed, ingest stages are excluded (the cause is downstream)
    */
  private def computeBottleneck(snap: V2BackpressureSnapshot): Option[BpSignal] = {
    val signals = scala.collection.mutable.ArrayBuffer[BpSignal]()
    // SQ output destinations: rank 6 (most downstream — the actual slow sink)
    snap.standingQueries.foreach { sq =>
      sq.outputs.foreach { o =>
        val outputKey = s"${sq.name}/${o.name}"
        o.destinations.foreach { d =>
          signals += BpSignal("sq-output", outputKey, s"Output: ${o.name}", d.state, 6)
        }
        // SQ output enrichment: rank 5
        o.enrichmentState.foreach { state =>
          signals += BpSignal("sq-enrichment", outputKey, s"Output: ${o.name}", state, 5)
        }
      }
      // SQ queues: rank 4
      val qi = sq.queue
      val stt =
        if (qi.thresholdRatio >= 1.0) "BACKPRESSURED"
        else if (qi.thresholdRatio >= 0.4) "CONSTRAINED"
        else "FLOWING"
      signals += BpSignal("sq", sq.name, s"Standing Query: ${sq.name}", stt, 4)
    }
    // Ingest stages (only when valve is open — when closed, the cause is downstream).
    // Only actively-running ingests can be a bottleneck: paused/restored/failed/terminal ingests
    // aren't processing, and their gauges are stale or absent.
    if (snap.globalValve.isOpen) {
      snap.ingests.filter(_.status == "RUNNING").foreach { p =>
        signals += BpSignal("gw", p.name, s"Graph: ${p.name}", p.stages.preGraphWrite, 3)
        p.stages.postGraphWrite.foreach { s =>
          signals += BpSignal("ack", p.name, s"Graph: ${p.name}", s, 3)
        }
      }
    }
    // Only "BACKPRESSURED" counts as a bottleneck; CONSTRAINED is informational only
    signals.filter(_.state == "BACKPRESSURED").sortBy(-_.rank).headOption
  }

  private def systemStatus(snap: V2BackpressureSnapshot, bottleneck: Option[BpSignal]): (String, String) =
    if (!snap.globalValve.isOpen) ("Stopped", C.red)
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
  private val ViewModeKey = "bpc_viewMode"
  private val NamespacesKey = "bpc_namespaces"
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

  /** @param clusterSignal If defined, the diagram shows a cluster card above the Q node.
    *                       Only present when running Quine Enterprise with cluster target size > 1.
    */
  def apply(
    potSignal: Signal[Pot[V2BackpressureSnapshot]],
    clusterSignal: Option[Signal[Pot[V2ServiceStatus]]] = None,
  ): HtmlElement = {
    var lastRefreshMs: Double = {
      val stored = dom.window.localStorage.getItem(StorageKey)
      if (stored != null && stored.nonEmpty) scala.util.Try(stored.toDouble).getOrElse(js.Date.now())
      else js.Date.now()
    }
    val refreshTextVar = Var(s"every ${PollIntervalMs / 1000}s")
    val refreshColorVar = Var(C.green)

    // Enterprise-only controls — load previous selections from localStorage
    val storedViewMode = loadStorage(ViewModeKey).filter(v => v == "Member" || v == "Cluster").getOrElse("Member")
    val storedNamespaces: Set[String] = loadStorage(NamespacesKey)
      .map(_.split(',').filter(_.nonEmpty).toSet)
      .getOrElse(Set.empty)
    val viewModeVar = Var(storedViewMode)
    val namespacesVar: Var[Set[String]] = Var(storedNamespaces) // empty = all
    val allNamespacesVar: Var[Seq[String]] = Var(Seq.empty) // discovered from snapshot data
    val viewDropdownOpenVar = Var(false)
    val nsDropdownOpenVar = Var(false)
    val pausedVar = Var(false)

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

    // Only update lastRefreshMs on genuinely fresh data (Pot.Ready), not stale replays.
    // Compute client-side rates exactly once per tick via RateComputer.
    // When paused, the last enriched snapshot is held and new data is ignored.
    var lastEnriched: Option[EnrichedSnapshot] = None
    val enrichedSignal: Signal[Option[EnrichedSnapshot]] = potSignal.map { pot =>
      if (pausedVar.now()) lastEnriched
      else {
        pot match {
          case Pot.Ready(_) =>
            lastRefreshMs = js.Date.now()
            scala.util.Try(dom.window.localStorage.setItem(StorageKey, lastRefreshMs.toLong.toString))
            updateRefreshDisplay()
          case _ => ()
        }
        val enriched = pot.toOption.map(RateComputer.computeRates)
        // Update discovered namespaces from snapshot data and validate stored selections
        enriched.foreach { e =>
          val nss = (e.snap.ingests.map(_.namespace) ++ e.snap.standingQueries.map(_.namespace)).distinct.sorted
          if (nss != allNamespacesVar.now()) {
            allNamespacesVar.set(nss)
            // Prune any stored namespace selections that no longer exist on the backend
            val current = namespacesVar.now()
            if (current.nonEmpty) {
              val valid = current.intersect(nss.toSet)
              if (valid != current) {
                val updated = if (valid.isEmpty) Set.empty[String] else valid
                namespacesVar.set(updated)
                saveStorage(NamespacesKey, updated.mkString(","))
              }
            }
          }
        }
        lastEnriched = enriched
        enriched
      } // end else (not paused)
    }

    // Cluster info — updated from the optional cluster signal
    val clusterInfoVar: Var[Option[ClusterInfo]] = Var(None)

    // Heartbeat timer to update staleness display every second
    var heartbeatHandle: Int = 0

    div(
      width := "100%",
      styleAttr := "font-family:Inter,system-ui,sans-serif;color:#0a295b;",
      onMountCallback { _ =>
        heartbeatHandle = dom.window.setInterval(() => updateRefreshDisplay(), 1000)
      },
      onUnmountCallback { _ =>
        if (heartbeatHandle != 0) dom.window.clearInterval(heartbeatHandle)
      },
      // Subscribe to cluster status if available
      clusterSignal match {
        case Some(sig) =>
          onMountBind { (_: MountContext[HtmlElement]) =>
            sig --> Observer[Pot[V2ServiceStatus]] { pot =>
              val update: Option[ClusterInfo] = pot.toOption.flatMap { status =>
                if (status.cluster.targetSize > 1)
                  Some(ClusterInfo(status.cluster.clusterMembers.size, status.cluster.hotSpares.size, status.fullyUp))
                else None
              }
              clusterInfoVar.set(update)
            }
          }
        case None => emptyMod
      },
      // ── Status bar (HTML, above the SVG) ──
      div(
        styleAttr := "display:flex;align-items:stretch;gap:0;background:#ffffff;border:1px solid rgba(10,41,91,0.1);border-radius:14px;margin-bottom:14px;box-shadow:0 6px 22px rgba(10,41,91,0.07);",
        child <-- enrichedSignal.map {
          case None => div(styleAttr := "padding:13px 20px;color:#6c7390;", "Loading...")
          case Some(enriched) =>
            val snap = enriched.snap
            val nsF = namespacesVar.now()
            def nsVis(ns: String): Boolean = nsF.isEmpty || nsF.contains(ns)
            val filteredSnap = snap.copy(
              ingests = snap.ingests.filter(p => nsVis(p.namespace)),
              standingQueries = snap.standingQueries.filter(sq => nsVis(sq.namespace)),
            )
            val ingests0 = filteredSnap.ingests
            val bn = computeBottleneck(filteredSnap)
            val (sLabel, sColor) = systemStatus(snap, bn)
            val totalRate = ingests0.map(enriched.ingestRate).sum
            val open = snap.globalValve.isOpen
            div(
              styleAttr := "display:flex;align-items:stretch;width:100%;",
              // System status
              div(
                styleAttr := "display:flex;align-items:center;gap:11px;padding:13px 20px;border-right:1px solid rgba(10,41,91,0.08);",
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
                ),
              ),
              // Throughput
              div(
                styleAttr := "display:flex;flex-direction:column;justify-content:center;padding:13px 20px;border-right:1px solid rgba(10,41,91,0.08);cursor:default;",
                onMountCallback { ctx =>
                  LandingTooltip.attachToElement(
                    ctx.thisNode.ref,
                    () => {
                      val perIngest = ingests0.map { p =>
                        tipRow(p.name, s"${fmtExact(enriched.ingestRate(p))} ev/s")
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
                div(
                  styleAttr := "font-size:17px;font-weight:600;line-height:1.15;margin-top:2px;",
                  span(styleAttr := "color:#1658b7;font-weight:700;", fmtRate(totalRate)),
                  span(styleAttr := "color:#6c7390;font-weight:500;font-size:13px;", " / s"),
                ),
              ),
              // Bottleneck
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
              div(
                styleAttr := "display:flex;flex-direction:column;justify-content:center;padding:13px 20px;",
                div(
                  styleAttr := "font-size:11px;letter-spacing:0.16em;text-transform:uppercase;color:#6c7390;white-space:nowrap;",
                  "Global ingest valve",
                ),
                div(
                  styleAttr := "font-size:17px;font-weight:600;line-height:1.15;margin-top:2px;",
                  span(styleAttr := s"color:${if (open) C.green else C.red};", if (open) "Open" else "Closed"),
                  span(styleAttr := "color:#6c7390;font-weight:500;", " · "), {
                    val rc = snap.globalValve.oneMinuteClosures
                    val rcColor = if (rc > 10) C.red else "#0a295b"
                    span(
                      span(styleAttr := s"color:$rcColor;font-weight:700;", rc.toString),
                      span(styleAttr := "color:#6c7390;font-weight:500;font-size:13px;", " closed / min"),
                    )
                  },
                ),
              ),
              // Enterprise controls — combined "Selection" section (only when cluster signal is available)
              if (clusterSignal.isDefined)
                div(
                  styleAttr := "display:flex;flex-direction:column;justify-content:center;padding:13px 20px;border-left:1px solid rgba(10,41,91,0.08);position:relative;cursor:pointer;",
                  onClick --> { _ =>
                    val wasOpen = viewDropdownOpenVar.now() || nsDropdownOpenVar.now()
                    viewDropdownOpenVar.set(false)
                    nsDropdownOpenVar.set(!wasOpen)
                  },
                  div(
                    styleAttr := "font-size:11px;letter-spacing:0.16em;text-transform:uppercase;color:#6c7390;white-space:nowrap;",
                    "Selection",
                  ),
                  div(
                    styleAttr := "line-height:1.15;margin-top:2px;display:flex;align-items:center;gap:6px;white-space:nowrap;",
                    // M/C badge for view mode
                    child <-- viewModeVar.signal.map { view =>
                      val letter = if (view == "Cluster") "C" else "M"
                      val bg = if (view == "Cluster") "#3a4db8" else C.brite
                      span(
                        styleAttr := s"display:inline-flex;align-items:center;justify-content:center;width:20px;height:20px;border-radius:4px;background:$bg;color:#fff;font-size:11px;font-weight:700;flex-shrink:0;",
                        letter,
                      )
                    },
                    // Graph name(s)
                    child <-- namespacesVar.signal.map { selected =>
                      val all = allNamespacesVar.now()
                      val label =
                        if (selected.isEmpty || selected.size == all.size) "All Graphs"
                        else if (selected.size == 1) selected.head
                        else selected.toSeq.sorted.mkString(", ")
                      span(
                        styleAttr := "font-size:14px;font-weight:600;color:#0a295b;overflow:hidden;text-overflow:ellipsis;max-width:160px;",
                        label,
                      )
                    },
                    span(styleAttr := "font-size:10px;color:#6c7390;", "▾"),
                  ),
                  // Combined dropdown with click-outside backdrop
                  child.maybe <-- nsDropdownOpenVar.signal.combineWith(allNamespacesVar.signal).map { case (isOpen, allNs) =>
                    if (!isOpen) None
                    else
                      Some(
                        span(
                          // Invisible backdrop to catch clicks outside the dropdown
                          div(
                            styleAttr := "position:fixed;top:0;left:0;width:100vw;height:100vh;z-index:99;",
                            onClick.stopPropagation --> { _ => nsDropdownOpenVar.set(false) },
                          ),
                          div(
                            styleAttr := "position:absolute;top:100%;right:0;margin-top:4px;background:#ffffff;border:1px solid rgba(10,41,91,0.14);border-radius:10px;box-shadow:0 8px 24px rgba(10,41,91,0.12);z-index:100;min-width:200px;padding:8px;",
                            onClick.stopPropagation --> { _ => () }, // prevent clicks inside dropdown from bubbling
                            // View section
                            div(
                              styleAttr := "font-size:9px;letter-spacing:0.12em;text-transform:uppercase;color:#6c7390;padding:4px 12px;",
                              "View",
                            ),
                            div(
                              styleAttr := "display:flex;gap:4px;padding:2px 8px 8px;",
                              div(
                                styleAttr := "padding:5px 14px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600;color:#0a295b;background:rgba(22,88,183,0.1);",
                                "Member",
                              ),
                              div(
                                styleAttr := "padding:5px 14px;border-radius:6px;font-size:13px;color:#c2c8de;cursor:not-allowed;",
                                "Cluster",
                              ),
                            ),
                            // Divider
                            div(styleAttr := "height:1px;background:rgba(10,41,91,0.08);margin:2px 8px;"),
                            // Named Graphs section
                            div(
                              styleAttr := "font-size:9px;letter-spacing:0.12em;text-transform:uppercase;color:#6c7390;padding:8px 12px 4px;",
                              "Named Graphs",
                            ),
                            div(
                              styleAttr := "padding:2px 4px;",
                              // Per-namespace checkboxes
                              allNs.map { ns =>
                                div(
                                  styleAttr := "padding:5px 8px;border-radius:6px;cursor:pointer;font-size:13px;color:#0a295b;display:flex;align-items:center;gap:8px;white-space:nowrap;",
                                  child <-- namespacesVar.signal.map { selected =>
                                    val isSelected = selected.contains(ns) || selected.isEmpty
                                    span(
                                      styleAttr := s"width:14px;height:14px;border-radius:3px;border:1.5px solid ${if (isSelected) C.brite
                                      else "#c2c8de"};display:inline-flex;align-items:center;justify-content:center;font-size:10px;color:${C.brite};flex-shrink:0;",
                                      if (isSelected) "✓" else "",
                                    )
                                  },
                                  span(ns),
                                  onClick --> { _ =>
                                    val current = namespacesVar.now()
                                    val allNsList = allNamespacesVar.now()
                                    val effective = if (current.isEmpty) allNsList.toSet else current
                                    val updated = if (effective.contains(ns)) {
                                      val r = effective - ns
                                      if (r.size == allNsList.size) Set.empty[String] else r
                                    } else {
                                      val r = effective + ns
                                      if (r.size == allNsList.size) Set.empty[String] else r
                                    }
                                    namespacesVar.set(updated)
                                    saveStorage(NamespacesKey, updated.mkString(","))
                                  },
                                )
                              },
                              // "Select all" link
                              div(
                                styleAttr := "padding:6px 8px 2px;",
                                span(
                                  styleAttr <-- namespacesVar.signal.map { s =>
                                    val isAll = s.isEmpty
                                    s"font-size:12px;cursor:pointer;color:${if (isAll) "#c2c8de"
                                    else C.brite};font-weight:500;${if (isAll) "" else "text-decoration:underline;"}"
                                  },
                                  child.text <-- namespacesVar.signal
                                    .map(s => if (s.isEmpty) "All selected" else "Select all"),
                                  onClick --> { _ => namespacesVar.set(Set.empty); saveStorage(NamespacesKey, "") },
                                ),
                              ),
                            ),
                          ),
                        ),
                      ) // end Some(span(backdrop, dropdown))
                  },
                )
              else emptyNode,
              // Update section
              div(
                styleAttr := "display:flex;flex-direction:column;justify-content:center;padding:13px 20px;margin-left:auto;border-left:1px solid rgba(10,41,91,0.08);cursor:default;min-width:140px;",
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
                  // Pause/Play button — aligned with content row
                  div(
                    styleAttr := "cursor:pointer;padding:2px;margin-left:4px;display:flex;align-items:center;",
                    onClick.stopPropagation --> { _ => pausedVar.update(!_) },
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
        onMountBind { ctx =>
          val container = ctx.thisNode.ref
          enrichedSignal --> Observer[Option[EnrichedSnapshot]] {
            case None => ()
            case Some(enriched) =>
              // Render into a detached div, then move DOM nodes (preserving event listeners)
              val offscreen = dom.document.createElement("div").asInstanceOf[dom.HTMLElement]
              renderDiagram(offscreen, enriched, clusterInfoVar.now(), namespacesVar.now())
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
        // Re-render when namespace selection changes
        onMountBind { ctx =>
          val container = ctx.thisNode.ref
          namespacesVar.signal --> Observer[Set[String]] { _ =>
            lastEnriched.foreach { enriched =>
              val offscreen = dom.document.createElement("div").asInstanceOf[dom.HTMLElement]
              renderDiagram(offscreen, enriched, clusterInfoVar.now(), namespacesVar.now())
              offscreen.querySelector("svg") match {
                case svg: dom.Element => container.style.minHeight = s"${svg.getAttribute("height")}px"
                case _ =>
              }
              while (container.firstChild != null) container.removeChild(container.firstChild)
              while (offscreen.firstChild != null) container.appendChild(offscreen.firstChild)
            }
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
        div(
          styleAttr := "display:flex;align-items:center;gap:8px;",
          span(styleAttr := s"width:22px;height:7px;border-radius:2px;background:${C.amber};"),
          "Bottleneck",
        ),
        child <-- enrichedSignal.map { opt =>
          val hasAck = opt.exists(_.snap.ingests.exists(p => p.stages.postGraphWrite.isDefined))
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

  private def renderDiagram(
    container: dom.HTMLElement,
    enriched: EnrichedSnapshot,
    clusterInfo: Option[ClusterInfo],
    nsFilter: Set[String],
  ): Unit = {
    val snap = enriched.snap
    def nsVisible(ns: String): Boolean = nsFilter.isEmpty || nsFilter.contains(ns)
    val ingests = snap.ingests.filter(p => nsVisible(p.namespace)).sortBy(_.name)
    val sqs = snap.standingQueries.filter(sq => nsVisible(sq.namespace)).sortBy(_.name)
    val outsFlat: Seq[(V2StandingQuery, V2SqOutput)] = sqs.flatMap(sq => sq.outputs.sortBy(_.name).map(o => (sq, o)))
    val totalOuts = outsFlat.size

    // Helper to get client-computed rate for an ingest or output
    def iRate(p: V2IngestSnapshot): Double = enriched.ingestRate(p)
    def oRate(sq: V2StandingQuery, o: V2SqOutput): Double = enriched.sqOutputRate(sq.namespace, sq.name, o)
    def sqProdRate(sq: V2StandingQuery): Double =
      enriched.queueProductionRate(sq.namespace, sq.name, sq.queue.productionRate)
    def sqConsRate(sq: V2StandingQuery): Double =
      enriched.queueConsumptionRate(sq.namespace, sq.name, sq.queue.consumptionRate)

    val showCluster = clusterInfo.isDefined
    val rows = math.max(math.max(ingests.size, totalOuts), 2)
    val bandHalf = (rows - 1) * RowGap / 2.0
    val ingestHalf = math.max(0, (ingests.size - 1) * 13).toDouble
    val gHalf = math.round(math.max(56, ingestHalf + 24)).toDouble
    val persCardH = 72
    val clusterH = 82.0
    val loopGap = math.max(104, bandHalf * 0.6)
    val clusterGap = math.max(86, bandHalf * 0.4)
    val aboveExtent =
      if (showCluster) math.max(bandHalf + 40, gHalf + clusterGap + clusterH / 2.0 + 6)
      else math.max(bandHalf + 40, gHalf + 30)
    val belowExtent = math.max(bandHalf + 100, gHalf + loopGap + persCardH / 2.0 + 58)
    val cx = 660.0
    val cy = aboveExtent
    val H = aboveExtent + belowExtent
    val top = cy - bandHalf
    val bot = cy + bandHalf

    val open = snap.globalValve.isOpen
    val filteredSnap = snap.copy(ingests = ingests, standingQueries = sqs)
    val bottleneck = computeBottleneck(filteredSnap)
    val (statusLabel, statusColor) = systemStatus(filteredSnap, bottleneck)
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
      outsFlat.map { case (sq, o) => oRate(sq, o) }.maxOption.getOrElse(1.0) * 1.2,
      100.0,
    )

    val gLeft = cx - 46; val gRight = cx + 46; val gTopY = cy - gHalf; val gBotY = cy + gHalf
    val gnX = gLeft - 2 // write tab X
    val cardX = Edge.toDouble; val cardR = cardX + BoxW
    val obX = W - Edge - BoxW; val obW = BoxW
    val persCenterY = (cy + gHalf) + loopGap
    val clCenterY = (cy - gHalf) - clusterGap // cluster card above Q

    val sL = distributeYs(ingests.size, top, bot)
    val oL = distributeYs(totalOuts, top, bot)
    val nHalf = ingestHalf
    val nB = distributeYs(ingests.size, cy - nHalf, cy + nHalf)

    // Assign Y positions to SQ outputs
    val outsWithY = outsFlat.zipWithIndex.map { case ((sq, o), idx) => (sq, o, oL.lift(idx).getOrElse(cy)) }
    def sqCenterY(sq: V2StandingQuery): Double = {
      val ys = outsWithY.filter(_._1.name == sq.name).map(_._3)
      if (ys.nonEmpty) ys.sum / ys.size else cy
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
      val amberFlow = !notFlowing && p.stages.preGraphWrite != "FLOWING"
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
      p.stages.postGraphWrite.foreach { s =>
        val ackBP = s == "BACKPRESSURED"
        val ac = stateColor(s)
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
            tipHeader(s"Ack — ${p.name}") +
            tipKv(tipRow("post-graph-write", s, Some(stateColor(s)))) +
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
      val gy = cy + (sqY - cy) * 0.16
      // Use client-computed production rate for the Q→queue ribbon
      val sqProdRateVal = sqProdRate(sq)
      val w = thick(sqProdRateVal, sqProductionRateRef)
      val over = sq.queue.thresholdRatio >= 1.0
      val isBind = bottleneck.exists(bp => bp.signalType == "sq" && bp.id == sq.name)
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
      outsWithY.filter(_._1.name == sq.name).foreach { case (_, o, oy) =>
        val outRate = oRate(sq, o)
        val dw = thick(outRate, sqOutputRateRef)
        val outFlowing = outRate > 1
        val ribbonAmber = o.enrichmentState match {
          case Some(st) => st == "BACKPRESSURED" || st == "CONSTRAINED" // enrichment is next stage
          case None =>
            o.destinations
              .exists(d => d.state == "BACKPRESSURED" || d.state == "CONSTRAINED") // destinations are next stage
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
      val rc = snap.globalValve.oneMinuteClosures
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
      val overThresholdSqs = sqs.filter(_.queue.thresholdRatio >= 1.0).map(_.name)
      val valveTipBody = () => {
        val base = tipHeader("Global ingest valve") +
          tipKv(
            tipRow("State", if (open) "Open" else "Closed", Some(vc)) +
            tipRow("Active closes", snap.globalValve.closedCount.toString) +
            tipRow("Closures (60s)", snap.globalValve.oneMinuteClosures.toString, Some(C.amber)),
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
              if (p.stages.preGraphWrite == "FLOWING") C.brite else C.amber
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
      val srcState = p.stages.source
      val statusTint = ingestStatusTint(p.status)
      val notFlowing = statusTint.isDefined
      val nameTxt = ellipsize(p.name, 18)
      val pRate = iRate(p); val rt = fmtRate(pRate)
      val accentColor = statusTint.getOrElse(stateColor(srcState))
      val textColor = statusTint.getOrElse(C.ink)
      val rateColor = statusTint.getOrElse(C.brite)
      val cardEl = svg.append("g").attr("filter", "url(#cardSh)")
      cardEl
        .append("clipPath")
        .attr("id", s"cl${p.name.hashCode}")
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
        .attr("clip-path", s"url(#cl${p.name.hashCode})")
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
      cardEl
        .append("text")
        .attr("x", tcx)
        .attr("y", y - 6)
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
      def stageRow(label: String, raw: String): String =
        if (notFlowing) tipRow(label, p.status, statusTint)
        else tipRow(label, raw, Some(stateColor(raw)))
      tipOn(
        cardEl.node().asInstanceOf[dom.Element],
        () =>
          tipHeader(s"Ingest — ${p.name}") +
          tipKv(
            tipRow("Status", p.status, statusTint) +
            stageRow(s"Source · ${p.sourceType}", srcState) +
            stageRow("pre-graph-write", p.stages.preGraphWrite) +
            p.stages.postGraphWrite.map(st => stageRow("post-graph-write", st)).getOrElse("") +
            tipRow("Throughput", s"${fmtExact(pRate)} ev/s") +
            tipRow("Avg (1m)", s"${fmtExact(p.rate)} ev/s") +
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
    val anyGwBp = ingests.exists(_.stages.preGraphWrite == "BACKPRESSURED")
    val clusterDegraded = clusterInfo.exists(!_.fullyUp)
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
    graphEl
      .append("image")
      .attr("href", ServiceIcons.quineIconSvg)
      .attr("x", cx - 30)
      .attr("y", cy - 30)
      .attr("width", 60)
      .attr("height", 60)
      .attr("preserveAspectRatio", "xMidYMid meet")
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
      val pc = stateColor(p.stages.preGraphWrite)
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
          val gwState = p.stages.preGraphWrite
          tipHeader(s"Graph write — ${p.name}") +
          tipKv(
            tipRow("pre-graph-write", gwState, Some(stateColor(gwState))) +
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
      val over = sq.queue.thresholdRatio >= 1.0
      val queueEl = svg.append("g")
      queueEl
        .append("text")
        .attr("x", QueueX + QmW / 2)
        .attr("y", mTop - 9)
        .attr("text-anchor", "middle")
        .attr("fill", C.ink)
        .attr("font-size", 15.5)
        .attr("font-weight", 700)
        .text(ellipsize(sq.name, 20))
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
        val qi = sq.queue
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
      val pct = math.round(sq.queue.thresholdRatio * 100).toInt
      val qc = if (over) C.red else if (pct >= 50) C.amber else C.brite
      tipOn(
        queueEl.node().asInstanceOf[dom.Element],
        () => {
          val qi = sq.queue
          tipHeader(s"SQ result queue — ${sq.name}") +
          tipKv(
            tipRow("Buffered", f"${qi.bufferCount}%,d") +
            tipRow("Threshold", qi.backpressureThreshold.toString, if (over) Some(C.red) else None) +
            tipRow("Max capacity", f"${qi.maxSize}%,d") +
            tipRow("Threshold fill", s"$pct%", Some(qc)) +
            tipRow("Production rate", s"${fmtExact(sqProdRate(sq))} / s") +
            tipRow("Consumption rate", s"${fmtExact(sqConsRate(sq))} / s") +
            tipRow("Avg prod (1m)", s"${fmtExact(qi.productionRate)} / s") +
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
      // Extract per-destination types and their states
      val destSlugs = o.destinations.map(d => (d.`type`, d.state))
      val worstDestState =
        if (o.destinations.exists(_.state == "BACKPRESSURED")) "BACKPRESSURED"
        else if (o.destinations.exists(_.state == "CONSTRAINED")) "CONSTRAINED"
        else "FLOWING"
      val wc = stateColor(worstDestState)
      val outRateVal = oRate(sq, o); val rt = fmtRate(outRateVal)
      val cardEl = svg.append("g").attr("filter", "url(#cardSh)")
      val cid = s"clo${o.name.hashCode}"
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
        val singleState = destSlugs.headOption.map(_._2).getOrElse("FLOWING")
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
        val sorted = destSlugs.sortBy { case (_, state) =>
          if (state == "BACKPRESSURED") 2 else if (state == "CONSTRAINED") 1 else 0
        }
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
      val enrichKey = s"${sq.name}/${o.name}"
      val preWfState = o.enrichmentState.getOrElse("FLOWING")
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
            tipHeader(s"Enrichment — $outputName") +
            tipKv(
              tipRow("Enrichment stage", preWfState, Some(stateColor(preWfState))) +
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
      val isGating = o.destinations.exists(_.state == "BACKPRESSURED")
      val isEnrichmentBottleneck = preWfState == "BACKPRESSURED"
      tipOn(
        cardEl.node().asInstanceOf[dom.Element],
        () =>
          tipHeader(s"Output — ${sq.name}/$outputName") +
          tipKv(
            (if (showEnrichment) tipRow("enrichment", preWfState, Some(stateColor(preWfState))) else "") +
            o.destinations.map(d => tipRow(d.`type`, d.state, Some(stateColor(d.state)))).mkString +
            tipRow("Throughput", s"${fmtExact(outRateVal)} / s") +
            tipRow("Avg (1m)", s"${fmtExact(o.rate)} / s") +
            tipRow("Total processed", f"${o.totalCount}%,d") +
            (if (isGating) tipRow("Gating", "yes — slow destination", Some(C.amber))
             else if (isEnrichmentBottleneck) tipRow("Gating", "yes — slow enrichment query", Some(C.amber))
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
      svg
        .append("text")
        .attr("x", dnX - 9)
        .attr("y", (gBotY + pTopY) / 2 + 3)
        .attr("text-anchor", "end")
        .attr("fill", C.muted)
        .attr("font-size", 11.5)
        .text("persist")
      svg
        .append("text")
        .attr("x", upX + 9)
        .attr("y", (gBotY + pTopY) / 2 + 3)
        .attr("text-anchor", "start")
        .attr("fill", C.muted)
        .attr("font-size", 11.5)
        .text("restore")
      // Card
      val chipS = 48; val chipX = cardL + 8; val chipY = cardT + (pch - chipS) / 2
      val persEl = svg.append("g")
      val pcId = s"clp${snap.persistor.`type`.hashCode}"
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
      ServiceIcons.forType(snap.persistor.`type`) match {
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
          val mono = snap.persistor.`type` match {
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
        .text(snap.persistor.`type`)
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
        .text(f"${snap.persistor.writeLatencyMs}%.2f")
      latEl.append("tspan").attr("fill", C.muted).attr("font-size", 12.5).attr("font-weight", 500).text(" · r ")
      latEl
        .append("tspan")
        .attr("fill", C.ink)
        .attr("font-size", 13.5)
        .attr("font-weight", 700)
        .text(f"${snap.persistor.readLatencyMs}%.2f")
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
          tipHeader(s"Persistor — ${snap.persistor.`type`}") +
          tipKv(
            tipRow("Store type", snap.persistor.`type`) +
            tipRow("Write latency", f"${snap.persistor.writeLatencyMs}%.2f ms") +
            tipRow("Read latency", f"${snap.persistor.readLatencyMs}%.2f ms"),
          ) +
          tipNote("Quine writes every graph mutation here for durability and reads node state back on demand."),
      )
    }

    // ════════ CLUSTER CARD (above Q, only when running enterprise cluster) ════════
    clusterInfo.foreach { ci =>
      val clAccNormal = "#3a4db8"; val clLite = "#dadcf6"
      val clAcc = if (ci.fullyUp) clAccNormal else C.red
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
      // Line 2: Status dot + Operational/Degraded
      val healthColor = if (ci.fullyUp) C.green else C.red
      val healthLabel = if (ci.fullyUp) "Operational" else "Degraded"
      clEl.append("circle").attr("cx", ctx + 3).attr("cy", clTop + 36).attr("r", 3).attr("fill", healthColor)
      clEl
        .append("text")
        .attr("x", ctx + 11)
        .attr("y", clTop + 40)
        .attr("fill", healthColor)
        .attr("font-size", 12.5)
        .attr("font-weight", 600)
        .text(healthLabel)
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
      // Line 4: Hot spares
      val sparesEl = clEl.append("text").attr("x", ctx).attr("y", clTop + 72)
      sparesEl
        .append("tspan")
        .attr("fill", C.ink)
        .attr("font-size", 15)
        .attr("font-weight", 700)
        .text(ci.hotSpareCount.toString)
      sparesEl
        .append("tspan")
        .attr("fill", C.muted)
        .attr("font-size", 13)
        .attr("font-weight", 500)
        .text(s" Hot Spare${if (ci.hotSpareCount == 1) "" else "s"}")

      tipOn(
        clEl.node().asInstanceOf[dom.Element],
        () =>
          tipHeader("Quine cluster") +
          tipKv(
            tipRow("Status", if (ci.fullyUp) "Operational" else "Degraded", Some(healthColor)) +
            tipRow("Cluster members", ci.memberCount.toString) +
            tipRow("Hot spares", ci.hotSpareCount.toString),
          ) +
          tipNote(
            "This node is one member of a Quine cluster. Each peer runs the same pipeline over its own shard and exchanges graph state bidirectionally.",
          ),
      )
    }

    // ════════ BOTTLENECK TAG ════════
    // Position the pill on the specific node that is the bottleneck
    bottleneck.foreach { bp =>
      val (tagX, tagY) = bp.signalType match {
        case "gw" =>
          // On the Q node — positioned at the top edge
          (cx, gTopY - 13)
        case "ack" | "ingest" =>
          // On the ingest card
          val idx = ingests.indexWhere(_.name == bp.id)
          val yy = sL.lift(idx).getOrElse(cy)
          val ch = math.min(58, RowGap - 14).toDouble
          (cardX + BoxW / 2.0, yy - ch / 2 - 13)
        case "sq" =>
          // On the SQ queue
          val sqY0 = sqs.find(_.name == bp.id).map(sqCenterY).getOrElse(cy)
          (QueueX + QmW / 2.0, sqY0 - 64)
        case "sq-output" | "sq-enrichment" =>
          // On the output card — bp.id is "sqName/outputName"
          val outIdx = outsFlat.indexWhere { case (sq, o) => s"${sq.name}/${o.name}" == bp.id }
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
