package com.thatdot.quine.webapp.components.landing

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom

/** Shared singleton tooltip used across landing-page cards.
  *
  * A single fixed-position div is appended to `document.body` on first use and
  * reused by every card. Callers build HTML strings with the helpers below and
  * attach mouse handlers either via `attachToElement` (for raw DOM built by D3
  * or Plotly) or `attach` (Laminar binder mod).
  */
object LandingTooltip {

  // Brand-aligned status palette, shared with IngestsCard.
  val ColorRunning = "#1658b7"
  val ColorIdle = "#d6a100"
  val ColorPartialFail = "#e07a1f"
  val ColorFailed = "#dc3545"

  def statusColor(status: String): String = status match {
    case "Running" => "#00fa9a" // brighter green for dark tooltip background
    case "Failed" => "#ff6b6b"
    case "Paused" | "Restored" => "#ffc107"
    case _ => "#acacc9"
  }

  def escape(s: String): String =
    s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")

  def formatRate(rate: Double): String =
    if (rate < 1) f"$rate%.1f/s"
    else f"${rate.toLong}%,d/s"

  def formatCount(n: Long): String = f"$n%,d"

  /** Title bar at the top of a tooltip. */
  def header(title: String): String =
    s"""<div style="font-weight: 600; font-size: 16px; margin-bottom: 8px;
       |            border-bottom: 1px solid rgba(255,255,255,0.15); padding-bottom: 6px;">
       |  ${escape(title)}
       |</div>""".stripMargin

  /** Two-column label/value row for use inside `kvTable`. */
  def kvRow(label: String, value: String, valueColor: Option[String] = None): String = {
    val colorStyle = valueColor.fold("")(c => s"color: $c;")
    s"""<tr>
       |  <td style="padding: 2px 12px 2px 0; color: #acacc9;">${escape(label)}</td>
       |  <td style="padding: 2px 0; text-align: right; $colorStyle">${escape(value)}</td>
       |</tr>""".stripMargin
  }

  def kvTable(rows: String): String =
    s"""<table style="width: 100%; border-collapse: collapse; font-size: 14px;">
       |  $rows
       |</table>""".stripMargin

  /** Section subheader inside a tooltip. */
  def subheader(title: String): String =
    s"""<div style="padding: 6px 0 3px 0; font-weight: 600; font-size: 13px; color: #acacc9;">
       |  ${escape(title)}
       |</div>""".stripMargin

  /** Render a breakdown table: one row per item, plus a rollup row when items > `topN`.
    * `items` should already be sorted by descending rate (or relevance) by the caller.
    *
    * Each item carries `(name, status, rate)` where status is optional (empty string → no status column).
    * When `topN` is exceeded, a final row summarizes the remainder: `"…and N more (Σ rate)"`.
    */
  def breakdownTable(items: Seq[BreakdownItem], topN: Int = 10): String = {
    if (items.isEmpty) return ""
    val hasStatus = items.exists(_.status.nonEmpty)
    val shown = items.take(topN)
    val remainder = items.drop(topN)

    val rows = shown.map { it =>
      val statusCell =
        if (hasStatus) {
          val c = if (it.status.nonEmpty) statusColor(it.status) else "#acacc9"
          s"""<td style="padding: 2px 12px 2px 0; color: $c;">${escape(it.status)}</td>"""
        } else ""
      s"""<tr>
         |  <td style="padding: 2px 12px 2px 0; word-break: break-all;">${escape(it.name)}</td>
         |  $statusCell
         |  <td style="padding: 2px 0; text-align: right;">${escape(it.detail)}</td>
         |</tr>""".stripMargin
    }.mkString

    val rollup =
      if (remainder.nonEmpty) {
        val cols = if (hasStatus) 3 else 2
        s"""<tr>
           |  <td colspan="$cols" style="padding: 4px 0 2px 0; color: #acacc9; font-style: italic;">
           |    …and ${remainder.size} more
           |  </td>
           |</tr>""".stripMargin
      } else ""

    s"""<table style="width: 100%; border-collapse: collapse; font-size: 14px;">
       |  $rows
       |  $rollup
       |</table>""".stripMargin
  }

  final case class BreakdownItem(name: String, status: String, detail: String)
  object BreakdownItem {
    def apply(name: String, detail: String): BreakdownItem = BreakdownItem(name, "", detail)
  }

  // --- singleton element -------------------------------------------------

  private lazy val tooltipEl: dom.html.Div = {
    val el = dom.document.createElement("div").asInstanceOf[dom.html.Div]
    el.id = "landing-tooltip"
    el.setAttribute(
      "style",
      "display: none; position: fixed; z-index: 10000; background: #0a295b; " +
      "color: #f4f4f9; border-radius: 6px; padding: 12px 16px; font-size: 14px; " +
      "font-family: sans-serif; pointer-events: none; box-shadow: 0 4px 12px rgba(0,0,0,0.3); " +
      "max-width: 420px; min-width: 220px; max-height: 480px; overflow: auto;",
    )
    dom.document.body.appendChild(el)
    el
  }

  /** Position the tooltip relative to a rect, preferring the right side but flipping
    * to the left (and clamping to the viewport) when it would overflow.
    */
  def showNear(html: String, anchor: dom.DOMRect): Unit = {
    tooltipEl.innerHTML = html
    tooltipEl.style.display = "block"
    // Measure after content paint.
    val vw = dom.window.innerWidth
    val vh = dom.window.innerHeight
    val tw = tooltipEl.getBoundingClientRect().width
    val th = tooltipEl.getBoundingClientRect().height
    val preferredLeft = anchor.right + 8
    val finalLeft =
      if (preferredLeft + tw <= vw - 4) preferredLeft
      else math.max(4, anchor.left - tw - 8)
    val preferredTop = anchor.top
    val finalTop =
      if (preferredTop + th <= vh - 4) preferredTop
      else math.max(4, vh - th - 4)
    tooltipEl.style.left = s"${finalLeft}px"
    tooltipEl.style.top = s"${finalTop}px"
  }

  /** Position the tooltip near a mouse coordinate.
    * Emulates `showNear` using the given point as a zero-size anchor.
    */
  def showAt(html: String, clientX: Double, clientY: Double): Unit = {
    tooltipEl.innerHTML = html
    tooltipEl.style.display = "block"
    val vw = dom.window.innerWidth
    val vh = dom.window.innerHeight
    val tw = tooltipEl.getBoundingClientRect().width
    val th = tooltipEl.getBoundingClientRect().height
    val finalLeft =
      if (clientX + 12 + tw <= vw - 4) clientX + 12
      else math.max(4, clientX - tw - 12)
    val finalTop =
      if (clientY + th <= vh - 4) clientY
      else math.max(4, vh - th - 4)
    tooltipEl.style.left = s"${finalLeft}px"
    tooltipEl.style.top = s"${finalTop}px"
  }

  def hide(): Unit = {
    tooltipEl.style.display = "none"
    activeAnchor = null
  }

  // Element that owns the currently-shown tooltip. A global `mousemove` listener
  // hides the tooltip whenever the pointer moves outside of this element. Without
  // this, overlapping SVG hover zones (especially in the D3 overlays) can swallow
  // `mouseleave` events and leave tooltips visible when the mouse has already
  // moved elsewhere.
  private var activeAnchor: dom.Element = null
  private var globalListenerInstalled = false

  private def ensureGlobalListener(): Unit = {
    if (globalListenerInstalled) return
    globalListenerInstalled = true
    dom.document.addEventListener(
      "mousemove",
      ((ev: dom.MouseEvent) => {
        if (activeAnchor != null && tooltipEl.style.display == "block") {
          val r = activeAnchor.getBoundingClientRect()
          val x = ev.clientX
          val y = ev.clientY
          // Small tolerance so subpixel motion at an edge doesn't flicker.
          val slop = 2.0
          val inside = x >= r.left - slop && x <= r.right + slop &&
            y >= r.top - slop && y <= r.bottom + slop
          if (!inside) hide()
        }
      }): js.Function1[dom.MouseEvent, Unit],
    )
  }

  private def setActive(el: dom.Element): Unit = {
    activeAnchor = el
    ensureGlobalListener()
  }

  /** Wire `mouseenter`/`mouseleave` on an existing DOM element. `buildHtml` is called
    * on each hover so the tooltip can reflect up-to-date data.
    */
  def attachToElement(el: dom.Element, buildHtml: () => String): Unit = {
    el.addEventListener(
      "mouseenter",
      (_: dom.Event) => {
        setActive(el)
        showNear(buildHtml(), el.getBoundingClientRect())
      },
    )
    el.addEventListener("mouseleave", (_: dom.Event) => hide())
  }

  /** Variant for SVG paths/shapes whose axis-aligned bounding rect is a poor tooltip
    * anchor (e.g. long bezier links that stretch across a card). The tooltip is shown
    * at the mouse coordinate and then tracks the mouse as it moves within the element.
    * `mouseleave` hides it as usual.
    *
    * Note: this does NOT register with the global `activeAnchor` tracker, since the
    * element's axis-aligned bounding box would cause the global `mousemove` listener
    * to keep the tooltip visible over whitespace. Path elements get reliable
    * `mouseleave` events via the stroke hit-test, so the explicit handler below is
    * sufficient.
    */
  def attachToPath(el: dom.Element, buildHtml: () => String): Unit = {
    el.addEventListener(
      "mouseenter",
      ((ev: dom.MouseEvent) => {
        // Intentionally skip setActive() here — see comment above.
        activeAnchor = null
        showAt(buildHtml(), ev.clientX, ev.clientY)
      }): js.Function1[dom.MouseEvent, Unit],
    )
    el.addEventListener(
      "mousemove",
      ((ev: dom.MouseEvent) => {
        if (tooltipEl.style.display == "block") {
          // Reuse the current HTML — only reposition.
          val vw = dom.window.innerWidth
          val vh = dom.window.innerHeight
          val tw = tooltipEl.getBoundingClientRect().width
          val th = tooltipEl.getBoundingClientRect().height
          val finalLeft =
            if (ev.clientX + 12 + tw <= vw - 4) ev.clientX + 12
            else math.max(4, ev.clientX - tw - 12)
          val finalTop =
            if (ev.clientY + th <= vh - 4) ev.clientY
            else math.max(4, vh - th - 4)
          tooltipEl.style.left = s"${finalLeft}px"
          tooltipEl.style.top = s"${finalTop}px"
        }
      }): js.Function1[dom.MouseEvent, Unit],
    )
    el.addEventListener("mouseleave", (_: dom.Event) => hide())
  }

  /** Laminar modifier that wires hover tooltip to an element. */
  def attach[El <: HtmlElement](buildHtml: => String): Mod[El] =
    List[Mod[El]](
      onMouseEnter --> { ev: dom.MouseEvent =>
        val target = ev.currentTarget.asInstanceOf[dom.Element]
        setActive(target)
        showNear(buildHtml, target.getBoundingClientRect())
      },
      onMouseLeave --> { _: dom.MouseEvent => hide() },
    )
}
