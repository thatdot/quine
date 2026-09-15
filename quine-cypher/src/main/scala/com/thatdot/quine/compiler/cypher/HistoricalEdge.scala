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
  directionSpellings,
  extractEdgeTypes,
  extractNode,
  limitRows,
  matchesNoEdgeType,
  measureRows,
  requireJournalEnabled,
  splitPositionalOptions,
}
import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.cypher._
import com.thatdot.quine.graph.{DomainIndexEvent, EdgeEvent, LiteralOpsGraph, PropertyEvent}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineIdProvider}

/** Query the historical changes of half edges from a source node over time.
  *
  * The `history.edgeChanges` procedure allows you to query the complete history of changes to half edges
  * from a source node over time. This leverages Quine's event sourcing architecture to return all timestamps
  * when half edges were added or removed throughout their history. A half edge represents one side of a complete
  * edge relationship.
  *
  * == Signatures ==
  * With edge type filter:
  * {{{
  * CALL history.edgeChanges(node, other, edgeType)
  * YIELD action, edgeType, changeTime
  * }}}
  *
  * With several edge types, reporting an edge matching any one of them:
  * {{{
  * CALL history.edgeChanges(node, other, ["KNOWS", "WORKS_WITH"])
  * YIELD action, edgeType, changeTime
  * }}}
  *
  * Without edge type filter (all edge types):
  * {{{
  * CALL history.edgeChanges(node, other, null)
  * YIELD action, edgeType, changeTime
  * }}}
  *
  * @param node The source node of the half edges to query (can be a node or node ID)
  * @param other The target node of the half edges to query (can be a node or node ID)
  * @param edgeType The type/label of the half edges to query: one string, or a list of them to
  *                 report an edge matching '''any''' of those named — the value-level counterpart of
  *                 `MATCH (a)-[:KNOWS|WORKS_WITH]->(b)`, which cannot be written as a procedure
  *                 argument. `null`, or the argument omitted, returns all edge types. An empty list
  *                 matches no type, as `x IN []` does, rather than being an error or meaning "any"
  * @param direction Which direction of half edge to report, in any spelling the REST API accepts
  *                  (`Outgoing`/`outgoing`/`out`, and so on), or omitted/`null` for every direction
  * @param options Optional map of `since`, `through`, and `limit`. Arguments are positional, so a
  *                call giving options passes null for any earlier argument it is skipping
  * @return `action` - The action that occurred - either "added" or "removed"
  * @return `edgeType` - The type/label of the half edge that was changed
  * @return `direction` - How this node holds the half edge, in canonical capitalised form
  * @return `other` - The node at the far end of the half edge
  * @return `changeTime` - The timestamp when the half edge was changed (milliseconds since epoch)
  *
  * The procedure returns multiple rows, one for each time a half edge from the source node was modified,
  * sorted chronologically. This procedure only examines half edges from the perspective of the source node.
  *
  * == These are not necessarily traversable edges ==
  * A half edge reported here is what this one node recorded. Quine will not follow it unless the
  * node at the far end recorded its matching half: a traversal asks that node to confirm its half
  * before continuing, and a pattern match skips the edge when it cannot. So an edge appearing here
  * is not by itself an edge `MATCH` will walk.
  *
  * That is the point of reading one node rather than two — it is what makes this cheap, and it is
  * what shows a half edge whose counterpart was never written. To find when an edge became
  * traversable, take the `edgeType`, `direction`, and `other` from a row here and pass them to
  * `history.edgeChangesBetween`, which reads the far node too and reports the edge only for the periods
  * both nodes agreed it existed.
  *
  * == Scope ==
  * Every half edge the source node holds pointing at the target node is reported, whichever direction
  * it was stored with — `Outgoing`, `Incoming`, or `Undirected`. Each half edge is reported in its own
  * right, with no attempt to find the matching half on the target node, so half edges whose counterpart
  * is missing appear here just like complete ones. Surfacing partial and one-sided state is the purpose
  * of this procedure; use `history.edgeChangesBetween` when only whole edges are of interest.
  *
  * == Usage Examples ==
  *
  * Basic usage - query the complete change history of half edges of a specific type:
  * {{{
  * MATCH (alice:Person {name: "Alice"})
  * MATCH (bob:Person {name: "Bob"})
  * CALL history.edgeChanges(alice, bob, "KNOWS")
  * YIELD action, edgeType, changeTime
  * RETURN action, edgeType, datetime({epochMillis: changeTime}) AS changeDate
  * ORDER BY changeTime
  * }}}
  *
  * Query all half edge types from a source node:
  * {{{
  * MATCH (alice:Person {name: "Alice"})
  * MATCH (bob:Person {name: "Bob"})
  * CALL history.edgeChanges(alice, bob, null)
  * YIELD action, edgeType, changeTime
  * RETURN action, edgeType, datetime({epochMillis: changeTime}) AS changeDate
  * ORDER BY changeTime
  * }}}
  *
  * Temporal query integration - the procedure respects the historical moment being queried, which is
  * supplied by the `at-time` query parameter rather than by the query text:
  * {{{
  * MATCH (user:User {id: "user123"})
  * MATCH (role:Role {name: "admin"})
  * CALL history.edgeChanges(user, role, "HAS_ROLE")
  * YIELD action, edgeType, changeTime
  * RETURN datetime({epochMillis: changeTime}) AS roleChangeTime, action AS roleAction
  * ORDER BY changeTime
  * }}}
  *
  * == Implementation Notes ==
  * - Uses Quine's event sourcing infrastructure to read the node's journal of events
  * - Reports the `EdgeAdded` and `EdgeRemoved` events this node recorded, and no other node's
  * - Filters on the node at the far end, the edge type, and the direction, in that order; each is
  *   optional, and an omitted or null filter matches every value
  * - Results are reported in the order the node recorded them, which is chronological
  * - The procedure is idempotent and read-only
  * - Uses the efficient `getJournal()` literal command with optional temporal filtering
  * - Respects query execution time contexts (see the `at-time` query parameter)
  * - All timestamps are in milliseconds since Unix epoch
  * - If no half edges have ever existed, the procedure returns zero rows
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
  * - '''Streaming''': The journal is read as a stream and each event maps to at most one row, so no
  *   part of the history is accumulated and a node's journal may be arbitrarily long
  * - '''Source-Optimized''': Queries only the source node's journal for half edge events
  *
  * == Use Cases ==
  * The procedure is particularly useful for:
  * - '''Half Edge Audit Trails''': Track all changes to half edges from a specific node
  * - '''Source-Side Analysis''': Understand relationship changes from one node's perspective
  * - '''Debugging''': Investigate half edge creation and removal for troubleshooting
  * - '''Asymmetric Relationships''': Analyze relationships that may not be bidirectional
  * - '''Performance Testing''': Compare half edge behavior vs complete edge behavior
  * - '''Graph Evolution''': Track how relationships evolve from one node's viewpoint
  *
  * == Differences from history.edgeChangesBetween ==
  * - '''Scope''': Only looks at half edges held by this node, in every direction
  * - '''Verification''': Does not verify that corresponding half edges exist on the node at the far end
  * - '''Performance''': Faster as it only queries one node's journal
  * - '''Use Case''': Useful for understanding one-sided relationship changes
  * - '''Completeness''': `history.edgeChangesBetween` requires both half edges before reporting a whole edge
  */
object HistoricalEdge extends UserDefinedProcedure {
  val name = "history.edgeChanges"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false

  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector(
      "node" -> Type.Node,
      "other" -> Type.Node,
      "edgeType" -> Type.Anything, // a string, or a list of them meaning any of those named
      "direction" -> Type.Str,
      "options" -> Type.Map,
    ),
    outputs = Vector(
      "action" -> Type.Str,
      "edgeType" -> Type.Str,
      "direction" -> Type.Str,
      "other" -> Type.Str,
      "changeTime" -> Type.Integer,
    ),
    description =
      "Query the historical changes of the half edges a node holds, returning all timestamps when half edges were added or removed",
  )

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
    val metrics = location.graph.metrics.journalWalkMetrics(name)
    implicit val idProvider: QuineIdProvider = graph.idProvider

    def extractDirection(directionLike: Value, position: String): Option[EdgeDirection] = directionLike match {
      case Expr.Null => None
      case Expr.Str(spelling) =>
        Some(
          directionSpellings.getOrElse(
            spelling,
            throw CypherException.Runtime(
              s"`$name` expects one of " +
              s"${directionSpellings.keys.toVector.sorted.mkString("`", "`, `", "`")} " +
              s"or null as the $position argument, but got: $spelling",
            ),
          ),
        )
      case other =>
        throw CypherException.Runtime(
          s"`$name` expects a string or null as the $position argument, but got: $other",
        )
    }

    def extractOther(otherLike: Value): Option[QuineId] = otherLike match {
      case Expr.Null => None
      case other => Some(extractNode(other, name, "second"))
    }

    val (positional, options) = splitPositionalOptions(arguments, 5, name, HistoricalOptions.windowKeys)

    // The positional arguments are in the same order as `history.edgeChangesBetween` takes them, so a row
    // from here can be handed straight to it to find when that edge became traversable. The only
    // difference is that the node at the far end is optional here and required there.
    val (sourceNodeId, edgeTypeFilter, directionFilter, targetNodeFilter) = positional match {
      case Seq(nodeLike, otherLike, edgeTypeLike, directionLike) =>
        (
          extractNode(nodeLike, name, "first"),
          extractEdgeTypes(edgeTypeLike, name, "third"),
          extractDirection(directionLike, "fourth"),
          extractOther(otherLike),
        )
      case Seq(nodeLike, otherLike, edgeTypeLike) =>
        (
          extractNode(nodeLike, name, "first"),
          extractEdgeTypes(edgeTypeLike, name, "third"),
          None,
          extractOther(otherLike),
        )
      case Seq(nodeLike, otherLike) =>
        (extractNode(nodeLike, name, "first"), None, None, extractOther(otherLike))
      case Seq(nodeLike) => (extractNode(nodeLike, name, "first"), None, None, None)
      case other => throw wrongSignature(other)
    }

    // A filter that cannot match is answered from the arguments, without opening the journal. The
    // call is still measured, so it shows up as one that read nothing and reported nothing.
    if (matchesNoEdgeType(edgeTypeFilter)) Source.empty[Vector[Value]].via(measureRows(metrics))
    else
      // The journal arrives in ascending timestamp order and each event maps to at most one row, so
      // the whole procedure is a stateless filter over the stream — no history is ever accumulated.
      graph
        .literalOps(location.namespace)
        .getJournal(
          sourceNodeId,
          startingAt = options.since,
          endingAt = options.through,
          atTime = location.atTime,
        )
        .via(countJournalEvents(metrics))
        .mapConcat { nodeEvent =>
          val atTime = nodeEvent.atTime.millis
          nodeEvent.event match {
            case EdgeAdded(halfEdge)
                if matchesEdgePattern(halfEdge, targetNodeFilter, edgeTypeFilter, directionFilter) =>
              List(row("added", halfEdge, atTime))
            case EdgeRemoved(halfEdge)
                if matchesEdgePattern(halfEdge, targetNodeFilter, edgeTypeFilter, directionFilter) =>
              List(row("removed", halfEdge, atTime))
            case _: EdgeEvent => Nil // an edge that does not match the filter
            case _: PropertyEvent => Nil
            case _: DomainIndexEvent => Nil
          }
        }
        .via(limitRows(options))
        .via(measureRows(metrics))
  }

  private def row(action: String, halfEdge: HalfEdge, atTime: Long)(implicit
    idProvider: QuineIdProvider,
  ): Vector[Value] =
    Vector(
      Expr.Str(action),
      Expr.Str(halfEdge.edgeType.name),
      Expr.Str(directionName(halfEdge.direction)),
      Expr.Str(halfEdge.other.pretty),
      Expr.Integer(atTime),
    )

  private def matchesEdgePattern(
    halfEdge: HalfEdge,
    targetNodeFilter: Option[QuineId],
    edgeTypeFilter: Option[Set[Symbol]],
    directionFilter: Option[EdgeDirection],
  ): Boolean = {
    val connectsToTarget = targetNodeFilter.forall(_ == halfEdge.other)
    def typeMatches = edgeTypeFilter.forall(_.contains(halfEdge.edgeType))
    def directionMatches = directionFilter.forall(_ == halfEdge.direction)
    connectsToTarget && typeMatches && directionMatches
  }
}
