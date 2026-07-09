package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

/** Inline-SVG glyphs for the results surface. Kept in one place so the exact path
  * data from the design handoff lives together.
  */
object ResultsIcons {

  /** The canvas door's glyph: a docked panel — a rounded rect with a filled bottom band, echoing the
    * results panel resting at the bottom of the canvas.
    */
  def dockedPanel: SvgElement =
    svg.svg(
      svg.width := "17",
      svg.height := "17",
      svg.viewBox := "0 0 18 18",
      svg.fill := "none",
      svg.stroke := "currentColor",
      svg.strokeWidth := "1.5",
      svg.strokeLineJoin := "round",
      svg.rect(svg.x := "2.5", svg.y := "3", svg.width := "13", svg.height := "12", svg.rx := "2.5"),
      svg.path(
        svg.d := "M2.5 10.5 H15.5 V12.5 A2.5 2.5 0 0 1 13 15 H5 A2.5 2.5 0 0 1 2.5 12.5 Z",
        svg.fill := "currentColor",
        svg.stroke := "none",
      ),
    )

  /** Filter magnifier — coreUI. */
  def magnifier: HtmlElement = i(cls := "cil-magnifying-glass", fontSize := "14px")

  /** Export — coreUI download glyph. */
  def download: HtmlElement = i(cls := "cil-cloud-download", fontSize := "14px")

  /** Bookmark — the retention toggle: outline at rest, filled once kept. (Custom SVG — the coreUI
    * free set has no filled bookmark, and the fill *is* the state, so a stroke-color change alone
    * was imperceptible at this size.)
    */
  def bookmark(filled: Boolean): SvgElement =
    svg.svg(
      svg.width := "13",
      svg.height := "13",
      svg.viewBox := "0 0 16 16",
      svg.fill := (if (filled) "currentColor" else "none"),
      svg.stroke := "currentColor",
      svg.strokeWidth := "1.5",
      svg.strokeLineJoin := "round",
      svg.path(svg.d := "M4.5 2.5 H11.5 V13.5 L8 10.9 L4.5 13.5 Z"),
    )

  /** Source kind — a query run: the coreUI terminal glyph. */
  def query: HtmlElement = i(cls := "cil-terminal")

  /** Source kind — a live tap: a dot broadcasting two waves. (Custom — it pulses while live, and
    * there's no coreUI broadcast glyph that reads as well.)
    */
  def tap: SvgElement =
    svg.svg(
      svg.width := "14",
      svg.height := "14",
      svg.viewBox := "0 0 16 16",
      svg.fill := "none",
      svg.stroke := "currentColor",
      svg.strokeWidth := "1.5",
      svg.strokeLineCap := "round",
      svg.strokeLineJoin := "round",
      svg.circle(svg.cx := "5", svg.cy := "8", svg.r := "1.4", svg.fill := "currentColor", svg.stroke := "none"),
      svg.path(svg.d := "M8 5.4 A 3.4 3.4 0 0 1 8 10.6"),
      svg.path(svg.d := "M10.3 3.6 A 6 6 0 0 1 10.3 12.4"),
    )

  /** Source kind — an errored run: the coreUI warning glyph. */
  def alert: HtmlElement = i(cls := "cil-warning")

  /** Browser-style history navigation (header back/forward). */
  def arrowLeft: HtmlElement = i(cls := "cil-arrow-left")
  def arrowRight: HtmlElement = i(cls := "cil-arrow-right")

  /** Edit a query (chip trailing action). */
  def pencil: HtmlElement = i(cls := "cil-pencil")

  /** Restart an ended tap (chip trailing action). */
  def reload: HtmlElement = i(cls := "cil-reload")

  /** Collapse the panel (header chevron). */
  def chevronDown: HtmlElement = i(cls := "cil-chevron-bottom")

  /** Close / remove — coreUI ✕. */
  def close: HtmlElement = i(cls := "cil-x", fontSize := "13px")

  /** Add a tap (switcher entry row) — coreUI plus. */
  def plus: HtmlElement = i(cls := "cil-plus")

  /** Auto garbage-collection (switcher footer) — coreUI recycle. */
  def recycle: HtmlElement = i(cls := "cil-recycle", fontSize := "12px")

  /** Enrichment step in the add-tap pipeline — coreUI cog. */
  def gear: HtmlElement = i(cls := "cil-cog", fontSize := "11px")
}
