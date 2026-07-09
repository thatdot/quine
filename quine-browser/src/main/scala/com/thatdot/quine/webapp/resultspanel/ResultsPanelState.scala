package com.thatdot.quine.webapp.resultspanel

import scala.util.matching.Regex

import io.circe.Json

import com.thatdot.quine.routes.CypherQueryResult

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

  /** Query failed. `actions` carries optional recovery affordances (e.g. "Run again
    * as text query" when a tabular query was run in node mode).
    */
  final case class ErrorResult(error: StructuredError, actions: Seq[ResultsAction] = Seq.empty) extends ResultOutcome

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

/** A recovery action offered alongside an error (a labelled button). */
final case class ResultsAction(label: String, run: () => Unit)

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
  * chip and the error block).
  */
final case class ResultsContent(outcome: ResultOutcome, queryEcho: String)

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

/** A parsed location within a query that an error refers to. Every field is
  * optional because it is recovered best-effort from a free-text error message.
  */
final case class ErrorLocation(line: Option[Int], column: Option[Int], offset: Option[Int])

/** Best-effort structured view of a query error.
  *
  * There is no typed error channel from the backend — by the time an error reaches
  * the UI it is a bare message string (REST collapses `Invalid` errors with
  * `mkString`; the WebSocket path forwards a raw string). So `raw` is always the
  * full message and is always shown; `headline`, `kind`, and `location` are filled
  * in only when they can be recovered from the text.
  *
  * @param raw the full, unmodified error message (trimmed)
  * @param headline the human-readable message, with any exception prefix / trailing
  *                 location parenthetical stripped
  * @param kind the simple exception name (e.g. `CypherException.Compile`), if found
  * @param location line/column/offset within the query, if found
  * @param offendingQuery the query that produced the error (for the error block)
  */
final case class StructuredError(
  raw: String,
  headline: String,
  kind: Option[String],
  location: Option[ErrorLocation],
  offendingQuery: String,
)

object StructuredError {

  private val lineRe: Regex = """line\s+(\d+)""".r.unanchored
  private val columnRe: Regex = """column\s+(\d+)""".r.unanchored
  private val offsetRe: Regex = """offset:?\s*(\d+)""".r.unanchored

  /** Quine's own "<Kind> at {row}.{column}" position (see `CypherException.pretty`). */
  private val rowColRe: Regex = """\bat\s+(\d+)\.(\d+)""".r.unanchored

  /** A leading exception/error token, possibly nested with `.` or `$`. Quine emits
    * simple names ending in `Error` (e.g. `SyntaxError`); the JVM/REST path may emit
    * a fully-qualified `...CypherException$Compile`.
    */
  private val kindRe: Regex =
    """\b([A-Z][A-Za-z0-9_]*(?:Exception|Error)(?:[.$][A-Za-z0-9_]+)*)""".r.unanchored

  /** A trailing "(line 1, column 18 (offset: 17))"-style parenthetical. */
  private val trailingLocationRe: Regex = """\s*\(line\s+\d+.*$""".r

  /** A leading "<Kind> at R.C [Invalid input:] " noise prefix (Quine's `pretty`). */
  private val leadingNoiseRe: Regex =
    """^[\w.$]*(?:Exception|Error)\s+at\s+\d+\.\d+\s*(?:Invalid input:\s*)?""".r

  def parse(raw: String, query: String): StructuredError = {
    val trimmed = raw.trim
    val kind = kindRe.findFirstMatchIn(trimmed).map { m =>
      // Reduce a fully-qualified name to its simple form, e.g.
      // "com…CypherException$Compile" -> "CypherException.Compile".
      m.group(1).split('.').last.replace('$', '.')
    }
    val location = rowColRe.findFirstMatchIn(trimmed) match {
      case Some(m) => Some(ErrorLocation(Some(m.group(1).toInt), Some(m.group(2).toInt), None))
      case None =>
        val line = lineRe.findFirstMatchIn(trimmed).map(_.group(1).toInt)
        val column = columnRe.findFirstMatchIn(trimmed).map(_.group(1).toInt)
        val offset = offsetRe.findFirstMatchIn(trimmed).map(_.group(1).toInt)
        Option.when(line.nonEmpty || column.nonEmpty || offset.nonEmpty)(ErrorLocation(line, column, offset))
    }
    StructuredError(
      raw = trimmed,
      headline = deriveHeadline(trimmed),
      kind = kind,
      location = location,
      offendingQuery = query,
    )
  }

  /** Best-effort short headline from the first line: strip a leading "<Kind> at R.C
    * [Invalid input:]" prefix (Quine) or text up to a fully-qualified "Exception: "
    * prefix (JVM/REST), plus any trailing location parenthetical. Falls back to the
    * raw first line if stripping leaves nothing.
    */
  private def deriveHeadline(raw: String): String = {
    val firstLine = raw.linesIterator.nextOption().getOrElse(raw).trim
    val deNoised = leadingNoiseRe.findPrefixOf(firstLine) match {
      case Some(prefix) => firstLine.substring(prefix.length)
      case None =>
        firstLine.indexOf(": ") match {
          case -1 => firstLine
          case i => firstLine.substring(i + 2)
        }
    }
    val headline = trailingLocationRe.replaceAllIn(deNoised, "").trim
    if (headline.nonEmpty) headline else firstLine
  }
}
