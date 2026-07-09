package com.thatdot.quine.webapp.queryui

import com.thatdot.quine.webapp.QueryUiOptions

/** How should the query UI interpret the query?
  *
  * Extracted from QueryUi.scala so these framework-agnostic types
  * can be shared without depending on a specific UI framework.
  */
sealed abstract class UiQueryType
object UiQueryType {

  /** Query is text, and results should go in the green message bar */
  case object Text extends UiQueryType

  /** Query is a side-effectful text query (e.g. a SET with no RETURN). Executes via the same
    * text-path wire call as [[Text]], but on success the bottom result bar is not populated —
    * only the ~750ms query-bar color flash signals completion.
    */
  final case object SideEffectsText extends UiQueryType

  /** Query is text; results render in the node-properties popup near DOM coordinates (x, y).
    * `iconCode`/`iconColor` carry the source node's graph icon (empty code = no custom icon)
    * so the popup header can mirror it. Currently only driven by the "View properties" action —
    * non-node result shapes fall back to a raw results table.
    */
  final case class TextPopup(x: Double, y: Double, iconCode: String = "", iconColor: String = "") extends UiQueryType

  /** Query is for nodes/edges, and results should be spread across the canvas */
  case object Node extends UiQueryType

  /** Query is for nodes/edges, and results should explode out from one node
    *
    * @param explodeFromId from which node should results explode
    * @param syntheticEdgeLabel if set, also draw a purple dotted edge (with this
    *                           label) from the central node to all of the other nodes
    */
  final case class NodeFromId(explodeFromId: String, syntheticEdgeLabel: Option[String]) extends UiQueryType
}

/** The server's classification of the current query buffer, from the `quine/queryKind` LSP
  * verdict. Drives which run path the Query input uses and whether the run buttons are enabled.
  */
sealed abstract class QueryKind
object QueryKind {

  /** Returns nodes/edges; results spread across the graph canvas. */
  case object Node extends QueryKind

  /** Returns rows; results shown as a table. */
  case object Table extends QueryKind

  /** Side-effectful with no rows (e.g. a SET with no RETURN); runs via the text path but
    * suppresses the result panel.
    */
  case object SideEffects extends QueryKind

  /** Not yet classified: no language server, an empty buffer, or a parse error. The run buttons
    * stay disabled until the server returns a concrete verdict.
    */
  case object Unknown extends QueryKind

  /** Parses the wire string from the `quine/queryKind` verdict; unrecognized values (including a
    * version-skewed server) fall back to [[Unknown]] rather than guessing a run path.
    */
  def fromString(wire: String): QueryKind = wire match {
    case "node" => Node
    case "table" => Table
    case "sideEffects" => SideEffects
    case _ => Unknown
  }

  /** Whether the verdict permits a run: every classified kind is runnable, [[Unknown]] is not. */
  def isRunnable(kind: QueryKind): Boolean = kind != Unknown

  /** Whether results spread across the graph canvas ([[Node]]) rather than the text/table path. */
  def runsOnGraph(kind: QueryKind): Boolean = kind == Node

  /** The [[UiQueryType]] a verdict submits as, distinguishing the result-panel behavior of the
    * shared text wire path ([[SideEffects]] suppresses the panel; [[Table]] populates it).
    */
  def toUiQueryType(kind: QueryKind): UiQueryType = kind match {
    case Node => UiQueryType.Node
    case SideEffects => UiQueryType.SideEffectsText
    case Table | Unknown => UiQueryType.Text
  }
}

/** A 1-based caret/diagnostic position (Monaco convention): the line and column travel together
  * so the two numbers can't drift apart as separate arguments.
  */
final case class Position(line: Int, column: Int)

/** A node's position on the `vis` canvas, in canvas pixel coordinates. The x and y travel together
  * so the pair can't be transposed where a starting position is passed.
  */
final case class CanvasPosition(x: Double, y: Double)

/** Whether an edge is a real graph edge or a synthetic edge drawn to visualize an
  * explode-from-node result — a purple dotted line from the central node to each result node.
  */
sealed abstract class EdgeKind
object EdgeKind {
  case object Real extends EdgeKind
  case object Synthetic extends EdgeKind

  /** Whether the edge is synthetic, which drives its purple dotted styling. */
  def isSynthetic(kind: EdgeKind): Boolean = kind == Synthetic
}

/** Whether the current query buffer can be run, and if not, why. Drives the run buttons' disabled
  * state. ("Running" is not modeled here: while a query runs the button is replaced by the cancel
  * affordance, so it is gated separately on the running signal.)
  */
sealed abstract class RunAvailability
object RunAvailability {

  /** Authorized, classified, and free of errors — the run buttons are enabled. */
  case object Runnable extends RunAvailability

  /** The user lacks READ permission on the graph. */
  case object NotAuthorized extends RunAvailability

  /** The buffer has at least one error-severity diagnostic. */
  case object HasErrors extends RunAvailability

  /** The language server has not yet returned a concrete verdict (`QueryKind.Unknown`). */
  case object Unclassified extends RunAvailability

  /** Evaluates run availability, reporting the first blocking reason in precedence order:
    * authorization, then errors, then classification.
    */
  def evaluate(canRead: Boolean, hasErrors: Boolean, kind: QueryKind): RunAvailability =
    if (!canRead) NotAuthorized
    else if (hasErrors) HasErrors
    else if (!QueryKind.isRunnable(kind)) Unclassified
    else Runnable

  /** Whether a verdict blocks the run (anything other than [[Runnable]]). */
  def isBlocked(availability: RunAvailability): Boolean = availability != Runnable
}

/** How `vis` should structure nodes */
sealed abstract class NetworkLayout
object NetworkLayout {
  case object Graph extends NetworkLayout
  case object Tree extends NetworkLayout
}

/** How should queries be relayed to the backend? */
sealed abstract class QueryMethod
object QueryMethod {
  case object Restful extends QueryMethod
  case object RestfulV2 extends QueryMethod
  case object WebSocket extends QueryMethod
  case object WebSocketV2 extends QueryMethod

  def parseQueryMethod(options: QueryUiOptions): QueryMethod = {
    val useWs = options.queriesOverWs.getOrElse(false)
    val useV2Api = options.queriesOverV2Api.getOrElse(true)

    (useV2Api, useWs) match {
      case (true, true) => QueryMethod.WebSocketV2
      case (true, false) => QueryMethod.RestfulV2
      case (false, true) => QueryMethod.WebSocket
      case (false, false) => QueryMethod.Restful
    }
  }
}
