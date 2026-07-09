package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L.{Signal, Var}
import io.circe.Json

/** Per-viewer presentation state, created once per surface and reset (except the
  * view toggle and the CSV-flatten preference) whenever a new result is shown.
  *
  * Components never mutate these `Var`s directly — they emit [[ViewerCommand]]s that
  * the surface's dispatcher applies here. The one bit of logic that belongs to the
  * slice (sort toggling) lives in [[ViewerState.toggleSort]].
  */
final case class ViewerState(
  view: Var[ResultsView],
  selectedRow: Var[Option[Seq[Json]]],
  sortCol: Var[Option[Int]],
  sortDir: Var[SortDir],
  search: Var[String],
  filterOpen: Var[Boolean],
  exportOpen: Var[Boolean],
  csvFlat: Var[Boolean],
  colWidths: Var[Vector[Double]], // explicit per-column widths; empty = auto-fit layout
)
object ViewerState {
  def initial: ViewerState =
    ViewerState(
      view = Var(ResultsView.Table),
      selectedRow = Var(None),
      sortCol = Var(None),
      sortDir = Var(SortDir.Asc),
      search = Var(""),
      filterOpen = Var(false),
      exportOpen = Var(false),
      csvFlat = Var(false),
      colWidths = Var(Vector.empty),
    )

  /** Reset result-specific state for a fresh result (keep `view` and `csvFlat`). */
  def resetForNewContent(s: ViewerState): Unit = {
    s.selectedRow.set(None)
    s.sortCol.set(None)
    s.sortDir.set(SortDir.Asc)
    s.search.set("")
    s.filterOpen.set(false)
    s.exportOpen.set(false)
    s.colWidths.set(Vector.empty)
  }

  /** Sort on a column: first click sorts ascending, clicking the active column flips
    * the direction.
    */
  def toggleSort(s: ViewerState, col: Int): Unit =
    if (s.sortCol.now().contains(col))
      s.sortDir.update(d => if (d == SortDir.Asc) SortDir.Desc else SortDir.Asc)
    else {
      s.sortCol.set(Some(col))
      s.sortDir.set(SortDir.Asc)
    }
}

/** Read-only signal projection of a [[ViewerState]] handed to a viewer's components,
  * so they declare exactly what they read and never reach into the mutable slice.
  */
final case class ViewerReads(
  view: Signal[ResultsView],
  search: Signal[String],
  filterOpen: Signal[Boolean],
  exportOpen: Signal[Boolean],
  csvFlat: Signal[Boolean],
  sortCol: Signal[Option[Int]],
  sortDir: Signal[SortDir],
  colWidths: Signal[Vector[Double]],
  selectedRow: Signal[Option[Seq[Json]]],
)
object ViewerReads {
  def of(s: ViewerState): ViewerReads =
    ViewerReads(
      view = s.view.signal,
      search = s.search.signal,
      filterOpen = s.filterOpen.signal,
      exportOpen = s.exportOpen.signal,
      csvFlat = s.csvFlat.signal,
      sortCol = s.sortCol.signal,
      sortDir = s.sortDir.signal,
      colWidths = s.colWidths.signal,
      selectedRow = s.selectedRow.signal,
    )
}
