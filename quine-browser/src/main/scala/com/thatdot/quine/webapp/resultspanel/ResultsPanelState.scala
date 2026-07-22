package com.thatdot.quine.webapp.resultspanel

import io.circe.Json

import com.thatdot.quine.routes.{CypherQueryResult, QueryLanguage}

/** The kind of result currently shown in the results surface. */
sealed abstract class ResultOutcome
object ResultOutcome {

  /** Cypher results: columns + rows. */
  final case class Tabular(result: CypherQueryResult) extends ResultOutcome

  /** Non-tabular (e.g. Gremlin / text) results: a flat list of JSON values. */
  final case class TextResults(values: Seq[Json]) extends ResultOutcome

  /** Query succeeded but returned nothing. `columns` lets an empty table keep its
    * header row; `wasTabular` distinguishes an empty Cypher table from empty text.
    */
  final case class EmptyResult(wasTabular: Boolean, columns: Seq[String]) extends ResultOutcome

  /** Restored from a previous session. Row data is not preserved — only the query
    * text (in `ResultsContent.queryEcho`) survives. Click the query chip to re-run.
    */
  final case class Restored(errorMessage: Option[String] = None) extends ResultOutcome

  /** Reduce a now-known-empty outcome to its empty placeholder, preserving whether
    * it was tabular and its columns. Non-result outcomes pass through unchanged.
    */
  def toEmpty(outcome: ResultOutcome): ResultOutcome = outcome match {
    case Tabular(result) => EmptyResult(wasTabular = true, result.columns)
    case TextResults(_) => EmptyResult(wasTabular = false, Nil)
    case other => other
  }
}

/** Result-scoped intents emitted by one viewer's controls (header, table, drawer).
  * Each is interpreted against a single `ViewerState`; components never mutate that
  * state directly — they emit these and the surface's dispatcher applies them.
  */
sealed abstract class ViewerCommand
object ViewerCommand {
  final case class SetView(view: ResultsView) extends ViewerCommand
  case object OpenFilter extends ViewerCommand
  case object CloseFilter extends ViewerCommand
  final case class SetSearch(text: String) extends ViewerCommand
  case object ClearSearch extends ViewerCommand
  final case class ToggleSort(col: Int) extends ViewerCommand
  final case class SetColWidths(widths: Vector[Double]) extends ViewerCommand
  final case class SelectRow(values: Seq[Json]) extends ViewerCommand
  case object CloseRow extends ViewerCommand
  case object ToggleExport extends ViewerCommand
  case object CloseExport extends ViewerCommand
  case object ToggleCsvFlatten extends ViewerCommand
  case object CopyJson extends ViewerCommand
  case object CopyCsv extends ViewerCommand
  case object DownloadJson extends ViewerCommand
  case object DownloadCsv extends ViewerCommand
}

/** Every state change in the results surface, emitted by components and interpreted
  * by the surface's dispatcher. Viewer-scoped changes are wrapped in [[ResultsCommand.OnViewer]];
  * the rest are session- or layout-scoped.
  */
sealed abstract class ResultsCommand
object ResultsCommand {
  final case class OnViewer(cmd: ViewerCommand) extends ResultsCommand

  // Navigation — browser-style: Back returns to the source you were just viewing (query run or
  // tap), Forward re-traces. Backed by the visited stack ([[NavHistory]]), not chronology.
  case object Back extends ResultsCommand
  case object Forward extends ResultsCommand
  case object ToggleCurrentPin extends ResultsCommand
  case object ToggleHistory extends ResultsCommand // toggle the Sources panel (the chip disclosure)
  case object CloseHistory extends ResultsCommand
  // The switcher's search text, hosted in the top bar (which is the switcher's header). Narrows both
  // the taps and the query-run list; cleared whenever the switcher closes.
  final case class SetSwitcherSearch(text: String) extends ResultsCommand
  case object ToggleTapChooser extends ResultsCommand // toggle the picker's add-tap chooser (the "+ Add tap" tab)
  final case class OpenEntry(idx: Int) extends ResultsCommand
  final case class TogglePinAt(idx: Int) extends ResultsCommand

  // Sources (taps)
  final case class SelectTap(id: String) extends ResultsCommand
  final case class OpenTap(target: TapTarget) extends ResultsCommand // subscribe to a tap
  final case class CloseTap(id: String) extends ResultsCommand // unsubscribe the panel's tap on this source
  final case class StopTap(id: String) extends ResultsCommand // freeze this tap to a viewable snapshot
  final case class RestartTap(id: String) extends ResultsCommand // re-open a stopped tap's stream
  final case class RemoveTap(id: String) extends ResultsCommand // stop and drop the tap from the band

  // Layout
  final case class SetHeight(px: Double) extends ResultsCommand // absolute height from a grab-rail drag
  final case class EnsureHeight(px: Double) extends ResultsCommand // grow to at least px (e.g. picker opening)
  // Grab-rail double-click: snap to `comfortable`, or back to the prior height if already there.
  final case class ToggleHeight(comfortable: Double) extends ResultsCommand
  // The canvas door's click: an in-place show/hide toggle (the panel's one collapse control).
  // Hides the panel when it's on screen; otherwise restores whatever was showing — opening the
  // switcher only when there's nothing to show. The compound decision lives in the interpreter,
  // not the view.
  case object DoorToggle extends ResultsCommand

  // Host out
  final case class UseQuery(text: String) extends ResultsCommand // put this query into the host editor
}

/** A single result to render, plus the query that produced it (for the header echo
  * chip and the error block) and the language that run actually used (so an adhoc card
  * records its language truthfully — a Gremlin run must not produce a card claiming
  * Cypher).
  *
  * `runId`/`revision` are the content's cheap change identity: `runId` names the run
  * that produced it (minted via [[ResultsContent.nextRunId]] — once per query run, and
  * once per independently-created content such as a restored history entry or snapshot
  * card) and `revision` counts emissions within the run (WS text queries re-emit the
  * whole growing buffer once per batch). Consumers asking "did the shown content
  * change?" compare `(runId, revision)` instead of deep-comparing the outcome payload,
  * which grows with the result and would make each comparison O(rows).
  */
final case class ResultsContent(
  outcome: ResultOutcome,
  queryEcho: String,
  language: QueryLanguage,
  runId: Long,
  revision: Int = 0,
)

object ResultsContent {
  private var lastRunId: Long = 0L

  /** Mint a session-unique run id — never reused, so two contents with equal
    * `(runId, revision)` are guaranteed to be the same emission.
    */
  def nextRunId(): Long = {
    lastRunId += 1
    lastRunId
  }
}

/** Which representation a viewer is currently showing. */
sealed abstract class ResultsView
object ResultsView {
  case object Table extends ResultsView
  case object Json extends ResultsView
}

/** Sort direction for a column. */
sealed abstract class SortDir
object SortDir {
  case object Asc extends SortDir
  case object Desc extends SortDir
}
