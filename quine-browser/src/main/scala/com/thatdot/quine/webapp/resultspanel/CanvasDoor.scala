package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles

/** The small slice the canvas door reads (built by the store): whether the panel is on screen, and
  * the inset/dispatch it needs to place itself and open. Purpose-built so the door — which lives on
  * the canvas layer, not inside the surface — depends only on what it uses.
  */
final case class DoorReads(
  collapsed: Signal[Boolean],
  contentShown: Signal[Boolean],
  sourcesOpen: Signal[Boolean],
  historyEntries: Signal[Vector[HistoryEntry]],
  effectiveInset: Signal[Int],
  dispatch: Observer[ResultsCommand],
)

/** The canvas door: a persistent corner control that shows/hides the panel in place, so a
  * peek-at-results cycle never moves the cursor. While the panel is hidden it wears the
  * docked-panel glyph (restoring what was showing, or opening the switcher when there's nothing
  * to show — with no history it grows into a labelled first-run pill); while the panel is on
  * screen it flips to a down-chevron, layered over the panel's corner as the matching hide.
  */
object CanvasDoor {

  def apply(reads: DoorReads): HtmlElement = {
    val panelShown: Signal[Boolean] =
      Signal.combine(reads.collapsed, reads.contentShown, reads.sourcesOpen).map { case (collapsed, content, picker) =>
        !collapsed && (content || picker)
      }
    // First-run only while hidden: once the panel is up, the door is purely the hide control.
    val firstRun: Signal[Boolean] =
      panelShown.combineWith(reads.historyEntries).map { case (shown, es) => !shown && es.isEmpty }
    div(
      cls := Styles.resultsCanvasDoor,
      cls(Styles.resultsCanvasDoorFirstRun) <-- firstRun,
      right <-- reads.effectiveInset.map(px => s"${px + 20}px"),
      title <-- panelShown.combineWith(reads.contentShown).map {
        case (true, _) => "Hide results"
        case (false, true) => "Show results"
        case (false, false) => "View or add"
      },
      onClick --> (_ => reads.dispatch.onNext(ResultsCommand.DoorToggle)),
      span(
        cls := Styles.resultsDoorGlyph,
        child <-- panelShown.map(shown => if (shown) ResultsIcons.chevronDown else ResultsIcons.dockedPanel),
      ),
      child <-- firstRun.map(fr => if (fr) span(cls := Styles.resultsDoorLabel, "View or add") else emptyNode),
    )
  }
}
