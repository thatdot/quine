package com.thatdot.quine.compiler.cypher

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.compiler.cypher.HistoricalJournalSupport.{
  HistoricalOptions,
  countJournalEvents,
  extractNode,
  limitRows,
  measureRows,
  requireJournalEnabled,
  splitPositionalOptions,
}
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher._
import com.thatdot.quine.graph.{DomainIndexEvent, EdgeEvent, LiteralOpsGraph, PropertyEvent}
import com.thatdot.quine.model.{PropertyValue, QuineIdProvider}

/** Query the historical changes of properties on a node over time.
  *
  * The `history.propertyChanges` procedure allows you to query the complete history of property changes
  * on a node over time. This leverages Quine's event sourcing architecture to return all timestamps
  * and values when the specified properties were changed throughout their history.
  *
  * == Signatures ==
  * With property key filter:
  * {{{
  * CALL history.propertyChanges(node, propertyKey)
  * YIELD key, value, changeTime
  * }}}
  *
  * Without property key filter (all properties):
  * {{{
  * CALL history.propertyChanges(node)
  * YIELD key, value, changeTime
  * }}}
  *
  * @param node The node to query (can be a node or node ID)
  * @param propertyKey The name of the property to query, or omitted/`null` to return all properties
  * @param options Optional map of `since`, `through`, `limit`, and `unreadableAsNull`. Arguments are
  *                positional, so a call giving options passes null for any earlier argument it skips
  * @return `key` - The name of the property that was changed
  * @return `value` - The value of the property at the time it was changed, or null when removed
  * @return `previousValue` - What the property held immediately before this change, so far as the
  *         reported window shows: null for the first setting of a key within it
  * @return `changeTime` - The timestamp when the property was changed (milliseconds since epoch)
  *
  * The procedure returns multiple rows, one for each time a property was modified, sorted chronologically.
  *
  * == Usage Examples ==
  *
  * Basic usage - query the complete change history of a property:
  * {{{
  * MATCH (n:Person {name: "Alice"})
  * CALL history.propertyChanges(n, "age")
  * YIELD key, value, changeTime
  * RETURN value, datetime({epochMillis: changeTime}) AS changeDate
  * ORDER BY changeTime
  * }}}
  *
  * Property lifecycle analysis:
  * {{{
  * MATCH (n:Product {sku: "PROD-123"})
  * CALL history.propertyChanges(n, "price")
  * YIELD key, value, changeTime
  * WITH collect({time: changeTime, price: value}) AS priceHistory
  * RETURN
  *   size(priceHistory) AS totalChanges,
  *   priceHistory[0].price AS initialPrice,
  *   priceHistory[-1].price AS currentPrice
  * }}}
  *
  * Query every property on a node, grouping the changes by property name:
  * {{{
  * MATCH (n:Person {name: "Alice"})
  * CALL history.propertyChanges(n)
  * YIELD key, value, changeTime
  * RETURN key, count(*) AS changes, max(changeTime) AS lastChanged
  * ORDER BY changes DESC
  * }}}
  *
  * Temporal query integration - the procedure respects the historical moment being queried, which is
  * supplied by the `at-time` query parameter rather than by the query text:
  * {{{
  * MATCH (n:Account {id: "12345"})
  * CALL history.propertyChanges(n, "balance")
  * YIELD key, value, changeTime
  * RETURN datetime({epochMillis: changeTime}) AS transactionTime, value AS balance
  * ORDER BY changeTime
  * }}}
  *
  * == Implementation Notes ==
  * - Uses Quine's event sourcing infrastructure to read the node's journal of events
  * - Reports the `PropertySet` and `PropertyRemoved` events for the named property key, or for every
  *   key when none is named
  * - Property removals are represented as entries with `null` values
  * - A value that cannot be deserialized raises an error rather than being reported as `null`, so a
  *   removal is never confused with unreadable data. Pass `{unreadableAsNull: true}` to read the
  *   rest of a damaged journal, reporting such a value as null instead
  * - Results are reported in the order the node recorded them, which is chronological
  * - The procedure is idempotent and read-only
  * - Uses the efficient `getJournal()` literal command with optional temporal filtering
  * - Respects query execution time contexts (see the `at-time` query parameter)
  * - All timestamps are in milliseconds since Unix epoch
  * - If a property has never been set, the procedure returns zero rows
  * - Requires the journal to be enabled; raises an error when it is not
  * - `since` and `through` bound the reported changes, both inclusive of the whole millisecond they
  *   name. They are read straight into the journal query, so a `through` naming a moment that has
  *   not arrived is not an error: the journal holds nothing past the present, so it matches
  *   everything, while still excluding anything recorded after it should the query outlive it
  *
  * == Performance Characteristics ==
  * - '''Efficient''': Uses dedicated `getJournal()` command that fetches only journal events
  * - '''Temporal Filtering''': Filters events at the persistence layer when used with temporal queries
  * - '''Narrow''': Does not load full node state (properties, edges, standing queries)
  * - '''Sequential Access''': Leverages Quine's sequential event storage for optimal performance
  * - '''Streaming''': The journal is read as a stream. The only state carried is the last value seen
  *   per property key, bounded by how many distinct keys the node has rather than by history length
  *
  * == Use Cases ==
  * The procedure is particularly useful for:
  * - '''Audit Trails''': Track all changes to critical properties for compliance and debugging
  * - '''Data Lineage''': Understand how property values evolved over time
  * - '''Anomaly Detection''': Identify unusual patterns in property change frequency or values
  * - '''Temporal Analysis''': Analyze trends and patterns in property evolution
  * - '''Debugging''': Investigate when and how property values changed during incidents
  * - '''Compliance Reporting''': Generate reports showing property change history for regulatory requirements
  */
object HistoricalProperty extends UserDefinedProcedure {
  val name = "history.propertyChanges"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false

  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector(
      "node" -> Type.Node,
      "propertyKey" -> Type.Str,
      "options" -> Type.Map,
    ),
    outputs = Vector(
      "key" -> Type.Str,
      "value" -> Type.Anything,
      "previousValue" -> Type.Anything,
      "changeTime" -> Type.Integer,
    ),
    description =
      "Query the historical changes of properties on a node, returning all timestamps and values when the properties were changed",
  )

  /** A change to one property, as recorded in the node's journal.
    *
    * Every change is reported as a setting, with a removal reported as a setting to null, which is
    * how a removal is written in Cypher (`SET n.key = null`).
    *
    * @param previousValue what the property was set to before this change, so far as this window
    *                      shows: the value carried by a removal, or the previous reported setting.
    *                      Null when neither is available, which is the case for the first setting
    *                      of a key inside a bounded window.
    */
  final private case class PropertyChange(key: Symbol, value: Value, previousValue: Value, atTime: Long)

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

    val (positional, options) =
      splitPositionalOptions(arguments, 3, name, HistoricalOptions.windowKeys + HistoricalOptions.UnreadableAsNull)

    val (nodeId, propertyKeyFilter) = positional match {
      // node, propertyKey (specific property, or null for all)
      case Seq(nodeLike, propertyKeyLike) =>
        val propertyKey = propertyKeyLike match {
          case Expr.Null => None
          case Expr.Str(key) => Some(Symbol(key))
          case other =>
            throw CypherException.Runtime(
              s"`$name` expects a string or null as the second argument, but got: $other",
            )
        }
        (extractNode(nodeLike, name, "first"), propertyKey)

      // node (all properties)
      case Seq(nodeLike) => (extractNode(nodeLike, name, "first"), None)

      case other => throw wrongSignature(other)
    }

    def deserialize(key: Symbol, value: PropertyValue, atTime: Long): Value =
      value.deserialized match {
        case scala.util.Success(qv) => Expr.fromQuineValue(qv)
        // A value reported as null is indistinguishable from a removal, so by default an unreadable
        // one fails rather than quietly corrupting the audit trail this procedure exists to give.
        // A caller who would rather read the rest of a damaged journal can ask for null instead.
        case scala.util.Failure(_) if options.unreadableAsNull => Expr.Null
        case scala.util.Failure(err) =>
          throw CypherException.Runtime(
            s"`$name` could not deserialize the value of property `${key.name}` recorded at $atTime: " +
            s"${err.getMessage}. Pass `{${HistoricalOptions.UnreadableAsNull}: true}` to report it as null instead.",
          )
      }

    // The journal arrives in ascending timestamp order and each event maps to at most one row. The
    // only state carried is the last value seen per key, so that each row can report what the
    // property was set to before it; that is bounded by the number of distinct keys, not by the
    // length of the history.
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
            case PropertySet(key, value) if propertyKeyFilter.forall(_ == key) =>
              val newValue = deserialize(key, value, atTime)
              val previous = lastValue.getOrElse(key, Expr.Null)
              lastValue = lastValue.updated(key, newValue)
              List(PropertyChange(key, newValue, previous, atTime))
            case PropertyRemoved(key, removed) if propertyKeyFilter.forall(_ == key) =>
              // A removal records what it removed, so the value before the change is known even
              // when this is the first thing the window shows of that key.
              val previous = deserialize(key, removed, atTime)
              lastValue = lastValue.updated(key, Expr.Null)
              List(PropertyChange(key, Expr.Null, previous, atTime))
            case _: PropertyEvent => Nil // a property other than the one being filtered for
            case _: EdgeEvent => Nil
            case _: DomainIndexEvent => Nil
          }
        }
      }
      .via(limitRows(options))
      .via(measureRows(metrics))
      .map(change => Vector(Expr.Str(change.key.name), change.value, change.previousValue, Expr.Integer(change.atTime)))
  }
}
