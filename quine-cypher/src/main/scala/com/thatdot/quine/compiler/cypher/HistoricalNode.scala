package com.thatdot.quine.compiler.cypher

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.common.logging.Pretty.PrettyHelper
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.compiler.cypher.HistoricalJournalSupport.{
  HistoricalOptions,
  countJournalEvents,
  directionName,
  extractNode,
  limitRows,
  measureRows,
  requireJournalEnabled,
  splitPositionalOptions,
}
import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher._
import com.thatdot.quine.graph.{DomainIndexEvent, LiteralOpsGraph, PropertyEvent}
import com.thatdot.quine.model.{PropertyValue, QuineIdProvider}

/** Query everything a node recorded about itself over time.
  *
  * Where `history.propertyChanges` reports a node's property changes and `history.edgeChanges` its edge changes,
  * this reports both from a single reading of the node's journal. It is the whole of what one node
  * wrote down, in the order it wrote it.
  *
  * == Signature ==
  * {{{
  * CALL history.nodeChanges(node, options)
  * YIELD kind, detail, changeTime
  * }}}
  *
  * @param node The node to query (can be a node or node ID)
  * @param options Optional map of `since`, `through`, `limit`, and `unreadableAsNull`
  * @return `kind` - `"property"` or `"edge"`, saying which shape `detail` has
  * @return `detail` - the change itself, as a map whose keys depend on `kind`
  * @return `changeTime` - the timestamp of the change (milliseconds since epoch)
  *
  * == The shape of `detail` ==
  * A property change and an edge change have nothing in common beyond happening, so rather than one
  * wide row where most columns are null for any given change, the change is reported as a map and
  * `kind` says how to read it:
  *
  *   - `kind = "property"` — `{key, value, previousValue}`. Every change is a setting, with a
  *     removal reported as a setting to null, and `previousValue` is what the property held before.
  *   - `kind = "edge"` — `{action, edgeType, other, direction}`, where `action` is `"added"` or
  *     `"removed"` and `direction` is how this node holds its half.
  *
  * == Usage Examples ==
  *
  * Everything that happened to a node, most recent last:
  * {{{
  * MATCH (n:Account {id: "A1"})
  * CALL history.nodeChanges(n)
  * YIELD kind, detail, changeTime
  * RETURN datetime({epochMillis: changeTime}) AS at, kind, detail
  * }}}
  *
  * Because `detail` is a map its fields can be reached directly, so one call answers questions that
  * would otherwise need the property and edge procedures separately:
  * {{{
  * MATCH (n:Account {id: "A1"})
  * CALL history.nodeChanges(n, {since: 1693526400000})
  * YIELD kind, detail, changeTime
  * WHERE kind = "edge" AND detail.action = "removed"
  * RETURN detail.edgeType, detail.other, changeTime
  * }}}
  *
  * == Implementation Notes ==
  * - Reads the node's journal once, where asking the property and edge procedures separately would
  *   read it twice
  * - Reports the half edges this node recorded, without checking the node at the far end, exactly as
  *   `history.edgeChanges` does. An edge listed here is not necessarily one `MATCH` will traverse; pass
  *   a row's `edgeType`, `direction`, and `other` to `history.edgeChangesBetween` to find when it became
  *   traversable
  * - `since` and `through` bound the reported changes, both inclusive of the whole millisecond they
  *   name. There is no filter on edge type or direction here, as there is on the edge procedures;
  *   this reports everything the node recorded, and `WHERE` on `detail` narrows it
  * - The only state carried is the last value seen per property key, so that `previousValue` can be
  *   reported; that is bounded by how many distinct properties the node has, not by how long its
  *   history is
  * - Requires the journal to be enabled; raises an error when it is not
  */
object HistoricalNode extends UserDefinedProcedure {
  val name = "history.nodeChanges"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false

  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector(
      "node" -> Type.Node,
      "options" -> Type.Map,
    ),
    outputs = Vector(
      "kind" -> Type.Str,
      "detail" -> Type.Map,
      "changeTime" -> Type.Integer,
    ),
    description =
      "Query everything a node recorded about itself over time, reporting property and edge changes together",
  )

  private val PropertyKind = "property"
  private val EdgeKind = "edge"

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation,
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig,
  ): Source[Vector[Value], NotUsed] = {

    val graph = LiteralOpsGraph.getOrThrow(s"`$name` procedure", location.graph)
    requireJournalEnabled(graph, name)
    implicit val idProvider: QuineIdProvider = graph.idProvider
    val metrics = location.graph.metrics.journalWalkMetrics(name)

    val (positional, options) = splitPositionalOptions(
      arguments,
      2,
      name,
      HistoricalOptions.windowKeys + HistoricalOptions.UnreadableAsNull,
    )

    val nodeId: QuineId = positional match {
      case Seq(nodeLike) => extractNode(nodeLike, name, "first")
      case other => throw wrongSignature(other)
    }

    def deserialize(key: Symbol, value: PropertyValue, atTime: Long): Value =
      value.deserialized match {
        case scala.util.Success(qv) => Expr.fromQuineValue(qv)
        // A value reported as null is indistinguishable from a removal, so by default an unreadable
        // one fails rather than quietly corrupting the record this procedure exists to give.
        case scala.util.Failure(_) if options.unreadableAsNull => Expr.Null
        case scala.util.Failure(err) =>
          throw CypherException.Runtime(
            s"`$name` could not deserialize the value of property `${key.name}` recorded at $atTime: " +
            s"${err.getMessage}. Pass `{${HistoricalOptions.UnreadableAsNull}: true}` to report it as null instead.",
          )
      }

    def propertyRow(key: Symbol, value: Value, previousValue: Value, atTime: Long): Vector[Value] =
      Vector(
        Expr.Str(PropertyKind),
        Expr.Map(Seq("key" -> Expr.Str(key.name), "value" -> value, "previousValue" -> previousValue)),
        Expr.Integer(atTime),
      )

    def edgeRow(action: String, halfEdge: com.thatdot.quine.model.HalfEdge, atTime: Long): Vector[Value] =
      Vector(
        Expr.Str(EdgeKind),
        Expr.Map(
          Seq(
            "action" -> Expr.Str(action),
            "edgeType" -> Expr.Str(halfEdge.edgeType.name),
            "other" -> Expr.Str(halfEdge.other.pretty),
            "direction" -> Expr.Str(directionName(halfEdge.direction)),
          ),
        ),
        Expr.Integer(atTime),
      )

    // One reading of the journal covers both kinds of change, in the order the node recorded them.
    graph
      .literalOps(location.namespace)
      .getJournal(
        nodeId,
        startingAt = options.since,
        endingAt = options.through,
        atTime = location.atTime,
      )
      .via(countJournalEvents(metrics))
      .statefulMapConcat { () =>
        var lastValue = Map.empty[Symbol, Value]

        nodeEvent => {
          val atTime = nodeEvent.atTime.millis
          nodeEvent.event match {
            case PropertySet(key, value) =>
              val newValue = deserialize(key, value, atTime)
              val previous = lastValue.getOrElse(key, Expr.Null)
              lastValue = lastValue.updated(key, newValue)
              List(propertyRow(key, newValue, previous, atTime))
            case PropertyRemoved(key, removed) =>
              // A removal records what it removed, so the value before the change is known even
              // when this is the first thing the window shows of that key.
              val previous = deserialize(key, removed, atTime)
              lastValue = lastValue.updated(key, Expr.Null)
              List(propertyRow(key, Expr.Null, previous, atTime))
            case EdgeAdded(halfEdge) =>
              List(edgeRow("added", halfEdge, atTime))
            case EdgeRemoved(halfEdge) =>
              List(edgeRow("removed", halfEdge, atTime))
            case _: PropertyEvent => Nil
            case _: DomainIndexEvent => Nil
          }
        }
      }
      .via(limitRows(options))
      .via(measureRows(metrics))
  }
}
