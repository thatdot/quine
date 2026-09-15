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
import com.thatdot.quine.graph.{DomainIndexEvent, LiteralOpsGraph, NodeEvent, PropertyEvent}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineIdProvider}

/** Query the historical changes of complete edges between two nodes over time.
  *
  * The `history.edgeChangesBetween` procedure allows you to query the complete history of changes to '''complete edges'''
  * between two nodes over time. This procedure verifies that both half edges (source→target and target→source)
  * exist before reporting an edge as complete. This leverages Quine's event sourcing architecture to return all
  * timestamps when complete edges were added or removed throughout their history.
  *
  * == Signatures ==
  * With edge type filter:
  * {{{
  * CALL history.edgeChangesBetween(node, other, edgeType)
  * YIELD action, edgeType, changeTime
  * }}}
  *
  * Without edge type filter (all edge types):
  * {{{
  * CALL history.edgeChangesBetween(node, other)
  * YIELD action, edgeType, changeTime
  * }}}
  *
  * The procedure returns multiple rows, one for each time a '''complete edge''' between the specified nodes
  * was modified, sorted chronologically. A complete edge requires both half edges (source→target and target→source)
  * to exist simultaneously.
  *
  * == This is the traversable view ==
  * Quine follows an edge only when both nodes hold their half: a traversal asks the far node to
  * confirm before continuing. This procedure applies that same rule over time, so the periods it
  * reports an edge as present are the periods a `MATCH` would have walked it. `history.edgeChanges` is
  * the cheaper one-node view and reports what a single node recorded, including halves whose
  * counterpart was never written; feed a row from it into this to find when that edge became
  * traversable.
  *
  * == Complete Edge Concept ==
  * In Quine's graph model, an edge consists of two '''half edges''':
  * 1. '''Source Half Edge''': Stored on the source node, pointing to the target, held as `Outgoing`
  * 2. '''Target Half Edge''': Stored on the target node, pointing back to the source, held as `Incoming`
  *
  * (An undirected edge is the same arrangement with both halves held as `Undirected`.)
  *
  * The `history.edgeChangesBetween` procedure only reports an edge as existing when '''both half edges''' are present.
  * This ensures data integrity and prevents reporting of incomplete or inconsistent edge states.
  *
  * '''Pairing Rules:'''
  * Two half edges belong to the same edge only when they agree on the edge type ''and'' their
  * directions are the reverse of each other. This matters whenever more than one edge connects the
  * same pair of nodes: `source -> target` and `target -> source` of the same type are two distinct
  * edges, as are a directed and an undirected edge of the same type, and each has its own lifecycle.
  * A half edge held by the target as `Outgoing` is the source half of the opposite edge, so it does
  * not complete an edge running `source -> target` and is not reported here.
  *
  * '''Timestamp Rules:'''
  * - '''Edge Creation''': Uses the '''later''' timestamp of the two half edge additions (when both halves exist)
  * - '''Edge Removal''': Uses the '''earlier''' timestamp of the two half edge removals (when the edge becomes incomplete)
  *
  * This approach ensures that reported edge lifetimes represent periods when the complete edge was actually
  * functional in the graph.
  *
  * == Usage Examples ==
  *
  * Basic usage - query the complete change history of a specific complete edge type:
  * {{{
  * MATCH (alice:Person {name: "Alice"})
  * MATCH (bob:Person {name: "Bob"})
  * CALL history.edgeChangesBetween(alice, bob, "KNOWS")
  * YIELD action, edgeType, changeTime
  * RETURN action, edgeType, datetime({epochMillis: changeTime}) AS changeDate
  * ORDER BY changeTime
  * }}}
  *
  * Relationship lifecycle analysis - track the complete lifecycle of a relationship:
  * {{{
  * MATCH (employee:Employee {id: "EMP123"})
  * MATCH (company:Company {name: "TechCorp"})
  * CALL history.edgeChangesBetween(employee, company, "WORKS_FOR")
  * YIELD action, edgeType, changeTime
  * RETURN
  *   datetime({epochMillis: changeTime}) AS eventDate,
  *   action,
  *   edgeType,
  *   CASE action
  *     WHEN "added" THEN "Employee hired"
  *     WHEN "removed" THEN "Employee left"
  *   END AS description
  * ORDER BY changeTime
  * }}}
  *
  * Query all edge types between two nodes:
  * {{{
  * MATCH (alice:Person {name: "Alice"})
  * MATCH (bob:Person {name: "Bob"})
  * CALL history.edgeChangesBetween(alice, bob, null)
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
  * CALL history.edgeChangesBetween(user, role, "HAS_ROLE")
  * YIELD action, edgeType, changeTime
  * RETURN datetime({epochMillis: changeTime}) AS roleChangeTime, action AS roleAction
  * ORDER BY changeTime
  * }}}
  *
  * == Implementation Notes ==
  * - Reads '''both nodes' ''' journals, except for a self loop, where one node holds both halves and
  *   a single journal is read
  * - Reconstructs complete edge lifecycle by tracking the state of both half edges over time
  * - Tracks each edge separately, so edges sharing a type and a pair of nodes do not interfere
  * - An undirected self loop is the one case where a single half edge '''is''' the whole edge: both
  *   halves are spelled identically, so only one is ever recorded
  * - Only reports edge state changes when the complete edge existence actually changes
  * - Results are reported in timestamp order, produced by merging the two journals rather than by sorting
  * - The procedure is idempotent and read-only
  * - Uses the efficient `getJournal()` literal command on both nodes with optional temporal filtering
  * - Respects query execution time contexts (see the `at-time` query parameter)
  * - All timestamps are in milliseconds since Unix epoch
  * - If no complete edges have ever existed, the procedure returns zero rows
  * - Requires the journal to be enabled; raises an error when it is not
  * - `since` and `through` bound the reported changes, both inclusive of the whole millisecond they
  *   name. They are read straight into the journal query, so a `through` naming a moment that has
  *   not arrived is not an error: the journal holds nothing past the present, so it matches
  *   everything, while still excluding anything recorded after it should the query outlive it
  * - `since` makes the answer approximate at the window's leading edge. See the section below
  *
  * == What a `since` bound costs in accuracy ==
  * Whether an edge is whole is a joint property of its two halves, held on two different nodes and
  * recorded in two different journals. A `since` bound starts both journal reads partway through,
  * so a half edge written before the window and never touched again is never read, and a single
  * half edge event inside the window no longer says on its own what it did to the edge. The two
  * cases are resolved the opposite way from each other, and the results are skewed in opposite
  * directions as a result:
  *
  * - '''Additions can be missed.''' An edge is reported as added only when '''both''' of its half
  *   edges are added inside the window. An edge whose two halves were written either side of the
  *   `since` bound is never reported as added, even though it did become whole inside the window.
  *   Querying back-to-back windows can therefore miss an edge's creation entirely, with neither
  *   window reporting it, ''even though the windows are contiguous''
  * - '''Removals can be over-reported.''' An edge is reported as removed whenever one of its half
  *   edges is removed inside the window and the other is not known to be already gone, because
  *   dropping either half is enough to break the edge and a removal proves its own half was held.
  *   A half edge that never had a counterpart — a dangling one being tidied up — therefore reports
  *   the removal of an edge that never existed. An edge whose two halves are '''both''' removed
  *   inside the window is still reported once, not twice
  *
  * Neither applies without `since`: an unbounded call reads each node's history from the beginning,
  * so every half edge's state is known and both answers are exact. Widening the window moves the
  * bound away from the changes of interest; omitting `since` removes the approximation altogether.
  * `through` and the `at-time` parameter do not have this effect, because they truncate the end of
  * the history rather than its beginning.
  *
  * == Performance Characteristics ==
  * - '''Dual-Journal Access''': Uses dedicated `getJournal()` command on both source and target nodes
  * - '''Temporal Filtering''': Filters events at the persistence layer when used with temporal queries
  * - '''Narrow''': Does not load full node state (properties, edges, standing queries)
  * - '''Sequential Access''': Leverages Quine's sequential event storage for optimal performance
  * - '''Streaming''': Both journals are read as streams and merged by timestamp, advancing whichever
  *   is earlier, so neither is accumulated. The only state carried is one entry per edge being
  *   tracked, bounded by how many edges run between the two nodes rather than by history length
  * - '''Windowed reads''': `since` is handed to the persistor as the start of the range, so the
  *   history before the window is never read at all — which is what the accuracy note above is the
  *   price of
  * - '''Event Correlation''': Reconstructs complete edge state by chronologically tracking both half edge states
  * - '''Complete Edge Verification''': Only reports edges when both half edges are verified to exist
  *
  * == Use Cases ==
  * The procedure is particularly useful for:
  * - '''Relationship Audit Trails''': Track all changes to critical relationships for compliance
  * - '''Access Control History''': Monitor when users gained/lost access to systems or resources
  * - '''Social Network Analysis''': Analyze friendship, following, or connection patterns over time
  * - '''Employment History''': Track hiring, transfers, and departures in organizational graphs
  * - '''Subscription Management''': Monitor customer subscription lifecycle events
  * - '''Security Monitoring''': Detect unusual patterns in relationship changes
  * - '''Data Lineage''': Track how data relationships evolved over time
  * - '''Debugging''': Investigate when and how specific relationships changed during incidents
  *
  * == Comparison with history.edgeChanges ==
  * Both report change over time from the journal; they differ in what counts as the thing that
  * changed. Each point below gives this procedure's behaviour first, then `history.edgeChanges`'.
  *
  * - '''Verification''': requires both half edges before reporting an edge, against reporting each
  *   half edge on its own, including one whose counterpart was never written
  * - '''Journals read''': two, one per node — or one for a self loop, where a single node holds
  *   both halves — against always one
  * - '''Unit reported''': one row per whole-edge state change, against one row per half edge change
  * - '''Timestamps''': the later of the two additions and the earlier of the two removals, so a
  *   reported lifetime is a period the edge was whole, against each half edge's own timestamp
  * - '''Use''': what `MATCH` could have traversed, against debugging, one-sided analysis, and
  *   dangling halves
  *
  * Neither collapses directions: `node -> other`, `other -> node`, and undirected are distinct
  * edges, each with its own lifecycle. What this procedure pairs is an edge's own two halves, which
  * the two nodes hold with opposite directions.
  *
  * '''When to use `history.edgeChangesBetween`:'''
  * - Tracking complete, functional relationships
  * - Data integrity is critical
  * - Need to ensure both sides of relationship exist
  * - Production analysis and reporting
  *
  * '''When to use `history.edgeChanges`:'''
  * - Debugging relationship issues
  * - Performance-critical single-node analysis
  * - Understanding one-sided relationship changes
  * - Investigating orphaned half edges
  *
  * @param node The source node of the edges to query (can be a node or node ID)
  * @param other The target node of the edges to query (can be a node or node ID)
  * @param edgeType The type/label of the edges to query: one string, or a list of them to report an
  *                 edge matching '''any''' of those named. `null`, or the argument omitted, returns
  *                 all edge types. An empty list matches no type, as `x IN []` does
  * @param direction Which direction of edge to report, in any spelling the REST API accepts
  *                  (`Outgoing`/`outgoing`/`out`, and so on), or omitted/`null` for every direction
  * @param options Optional map of `since`, `through`, and `limit`. Arguments are positional, so a
  *                call giving options passes null for any earlier argument it is skipping. `since`
  *                makes the answer approximate — see the accuracy note above
  * @return `action` - The action that occurred - either "added" or "removed"
  * @return `edgeType` - The type/label of the edge that was changed
  * @return `direction` - The direction the edge runs, in canonical capitalised form
  * @return `other` - The node at the far end of the edge
  * @return `changeTime` - The timestamp when the edge was changed (milliseconds since epoch)
  */
object HistoricalEdgeBetween extends UserDefinedProcedure {

  sealed trait EdgeAction
  object EdgeAction {
    case object Added extends EdgeAction
    case object Removed extends EdgeAction

    def toString(action: EdgeAction): String = action match {
      case Added => "added"
      case Removed => "removed"
    }
  }

  sealed trait HalfEdgeSide
  object HalfEdgeSide {
    case object Source extends HalfEdgeSide
    case object Target extends HalfEdgeSide

    /** Both halves at once.
      *
      * Only an undirected edge from a node to itself is like this. Both of its halves are spelled
      * the same way and both live on the same node, so there is only ever one half edge recorded,
      * and that one half edge is the entire edge.
      */
    case object Whole extends HalfEdgeSide
  }

  /** Identifies one edge between the two nodes, as the queried node records it.
    *
    * Several edges can connect the same pair of nodes, so the edge type alone does not say which
    * edge a half edge belongs to: `node -> other`, `other -> node`, and an undirected edge may all
    * share a type and must be replayed separately. The direction is the one the queried node holds,
    * which is what makes it a property of the edge rather than a restriction on reading it.
    */
  final private case class EdgeKey(edgeType: Symbol, direction: EdgeDirection)

  /** A change to one half of one edge, as recorded in the journal of the node holding that half. */
  final private case class HalfEdgeChange(action: EdgeAction, key: EdgeKey, atTime: Long, side: HalfEdgeSide)

  /** What the replay knows about one half of one edge. */
  sealed private trait HalfPresence
  private object HalfPresence {
    case object Held extends HalfPresence
    case object NotHeld extends HalfPresence

    /** Nothing in this call's reads says either way.
      *
      * Only a `since` bound produces this: it starts the journal read partway through the node's
      * history, so a half edge written before the window and never touched again is never seen. An
      * unbounded call sees every event a node ever recorded, so it starts from [[NotHeld]] and this
      * never arises.
      */
    case object Unobserved extends HalfPresence
  }

  /** State of one edge's two halves while its history is replayed. */
  final private case class EdgeState(sourceHalf: HalfPresence, targetHalf: HalfPresence) {

    /** Whether the edge is known to be whole. An unobserved half is not evidence that it is, so an
      * addition completing an edge whose other half was written before a `since` window is not
      * reported: the caller asked about a window that does not contain the edge's creation.
      */
    def isWhole: Boolean = sourceHalf == HalfPresence.Held && targetHalf == HalfPresence.Held

    /** Whether the edge counted as whole immediately before one of its halves was removed.
      *
      * A removal is evidence in itself: a half edge cannot be dropped unless it was held. The
      * counterpart may simply not have been read, and is taken to have been held — dropping either
      * half breaks the edge, so the alternative is to lose the removal entirely. See the note on
      * `since` in this object's documentation for what that costs.
      */
    def wasWholeBeforeRemoval: Boolean =
      sourceHalf != HalfPresence.NotHeld && targetHalf != HalfPresence.NotHeld

    def withHalf(side: HalfEdgeSide, action: EdgeAction): EdgeState = {
      val presence = action match {
        case EdgeAction.Added => HalfPresence.Held
        case EdgeAction.Removed => HalfPresence.NotHeld
      }
      side match {
        case HalfEdgeSide.Source => copy(sourceHalf = presence)
        case HalfEdgeSide.Target => copy(targetHalf = presence)
        // An undirected self loop's one half edge is the whole edge, so it moves both at once
        case HalfEdgeSide.Whole => copy(sourceHalf = presence, targetHalf = presence)
      }
    }
  }

  private object EdgeState {

    /** Where an edge's replay starts.
      *
      * With no `since` bound the journal is read from the beginning, so an edge not yet seen
      * genuinely has neither half. With one, the read starts partway through and nothing is known
      * about what came before.
      */
    def before(windowed: Boolean): EdgeState =
      if (windowed) EdgeState(HalfPresence.Unobserved, HalfPresence.Unobserved)
      else EdgeState(HalfPresence.NotHeld, HalfPresence.NotHeld)
  }

  val name = "history.edgeChangesBetween"
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
      "Query the historical changes of whole edges between two nodes, reporting an edge as added only once both nodes hold their half and as removed as soon as either drops it",
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

    def extractDirection(directionLike: Value): Option[EdgeDirection] = directionLike match {
      case Expr.Null => None
      case Expr.Str(spelling) =>
        Some(
          directionSpellings.getOrElse(
            spelling,
            throw CypherException.Runtime(
              s"`$name` expects one of " +
              s"${directionSpellings.keys.toVector.sorted.mkString("`", "`, `", "`")} " +
              s"or null as the fourth argument, but got: $spelling",
            ),
          ),
        )
      case other =>
        throw CypherException.Runtime(
          s"`$name` expects a string or null as the fourth argument, but got: $other",
        )
    }

    val (positional, options) = splitPositionalOptions(arguments, 5, name, HistoricalOptions.windowKeys)

    // Both nodes are required: reading the whole edge means reading both journals, so there is no
    // form that leaves the far end open. Discovering it instead would cost one journal read per
    // neighbour the node has ever had, which is what `history.edgeChanges` exists to avoid.
    val (sourceNodeId, targetNodeId, edgeTypeFilter, directionFilter) = positional match {
      case Seq(nodeLike, otherLike, edgeTypeLike, directionLike) =>
        (
          extractNode(nodeLike, name, "first"),
          extractNode(otherLike, name, "second"),
          extractEdgeTypes(edgeTypeLike, name, "third"),
          extractDirection(directionLike),
        )
      case Seq(nodeLike, otherLike, edgeTypeLike) =>
        (
          extractNode(nodeLike, name, "first"),
          extractNode(otherLike, name, "second"),
          extractEdgeTypes(edgeTypeLike, name, "third"),
          None,
        )
      case Seq(nodeLike, otherLike) =>
        (extractNode(nodeLike, name, "first"), extractNode(otherLike, name, "second"), None, None)
      case other => throw wrongSignature(other)
    }

    val literalOps = graph.literalOps(location.namespace)

    def journalOf(node: QuineId) =
      literalOps
        .getJournal(
          node,
          startingAt = options.since,
          endingAt = options.through,
          atTime = location.atTime,
        )
        .via(countJournalEvents(metrics))

    // A filter that cannot match is answered from the arguments, without opening either journal —
    // this procedure reads two, so the saving is the whole of both. The call is still measured, so
    // it shows up as one that read nothing and reported nothing.
    if (matchesNoEdgeType(edgeTypeFilter)) Source.empty[Vector[Value]].via(measureRows(metrics))
    else
      completeEdgeChanges(
        journalOf,
        windowed = options.since.isDefined,
        sourceNodeId,
        targetNodeId,
        edgeTypeFilter,
        directionFilter,
      ).via(limitRows(options))
        .via(measureRows(metrics))
  }

  /** Breaks ties between the two nodes' journals, which share only millisecond resolution. */
  private def sideRank(side: HalfEdgeSide): Int = side match {
    case HalfEdgeSide.Source => 0
    case HalfEdgeSide.Target => 1
    case HalfEdgeSide.Whole => 0 // never merged against anything: a self loop reads one journal
  }

  /** Stream the whole-edge changes for edges running from `sourceNodeId` to `targetNodeId`.
    *
    * Both journals arrive in ascending timestamp order, so they can be merged lazily rather than
    * collected. Only the per-edge half-edge state is carried along, which is bounded by the number
    * of distinct edges between the two nodes rather than by the length of their histories.
    */
  private def completeEdgeChanges(
    journalOf: QuineId => Source[NodeEvent.WithTime[NodeEvent], NotUsed],
    windowed: Boolean,
    sourceNodeId: QuineId,
    targetNodeId: QuineId,
    edgeTypeFilter: Option[Set[Symbol]],
    directionFilter: Option[EdgeDirection],
  )(implicit idProvider: QuineIdProvider): Source[Vector[Value], NotUsed] = {
    // A node at both ends of an edge holds both of its halves itself, so there is one journal to
    // read rather than two, and which half a record is cannot be told from which journal it came
    // out of. It has to be read off the half edge's own direction instead.
    val changes =
      if (sourceNodeId == targetNodeId)
        selfLoopChangesIn(journalOf(sourceNodeId), sourceNodeId, edgeTypeFilter, directionFilter)
      else {
        // Each node's journal contributes the half edge that node holds. Whichever journal is
        // being read, `halfEdge.other` is the node at the opposite end of the edge.
        val sourceChanges =
          halfEdgeChangesIn(journalOf(sourceNodeId), targetNodeId, edgeTypeFilter, directionFilter, HalfEdgeSide.Source)
        val targetChanges =
          halfEdgeChangesIn(journalOf(targetNodeId), sourceNodeId, edgeTypeFilter, directionFilter, HalfEdgeSide.Target)

        // The two journals belong to different nodes, so only their millisecond timestamps are
        // comparable: the lower bits of an EventTime are an actor-local sequence and mean nothing
        // across nodes. That leaves genuine ties, and the replay below is order-sensitive, so the
        // ordering is made total by putting the source node's half edge first at equal timestamps.
        sourceChanges.mergeSorted(targetChanges)(Ordering.by(change => (change.atTime, sideRank(change.side))))
      }

    changes
      .statefulMapConcat { () =>
        // Replay each edge on its own: half edges from different edges must never share a state
        // machine, or one edge's removal would look like another's.
        var states = Map.empty[EdgeKey, EdgeState]

        change => {
          val previous = states.getOrElse(change.key, EdgeState.before(windowed))
          val updated = previous.withHalf(change.side, change.action)
          states = states.updated(change.key, updated)

          // An addition and a removal are not mirror images here. An addition makes an edge whole
          // only when both halves are known to be held, so a half edge never read cannot complete
          // one. A removal breaks the edge whenever the counterpart is not known to be absent,
          // because dropping either half is enough and the removal proves its own half was held.
          val reported = change.action match {
            case EdgeAction.Added => Option.when(!previous.isWhole && updated.isWhole)(EdgeAction.Added)
            case EdgeAction.Removed => Option.when(previous.wasWholeBeforeRemoval)(EdgeAction.Removed)
          }

          reported.map { action =>
            Vector(
              Expr.Str(EdgeAction.toString(action)),
              Expr.Str(change.key.edgeType.name),
              Expr.Str(directionName(change.key.direction)),
              Expr.Str(targetNodeId.pretty),
              Expr.Integer(change.atTime),
            )
          }.toList
        }
      }
  }

  /** Collect, from the journal of a node that is both ends of an edge, the changes to that edge.
    *
    * The node holds every half itself, so which half a record is comes from the half edge's own
    * direction: the outgoing one is the half the edge leaves by and the incoming one is the half it
    * arrives by, and the edge is whole only once both are present. An undirected loop is different
    * again — both of its halves are spelled the same way, so the single half edge recorded is the
    * entire edge and there is no second half to wait for.
    */
  private def selfLoopChangesIn(
    journal: Source[NodeEvent.WithTime[NodeEvent], NotUsed],
    node: QuineId,
    edgeTypeFilter: Option[Set[Symbol]],
    directionFilter: Option[EdgeDirection],
  ): Source[HalfEdgeChange, NotUsed] =
    journal.mapConcat { nodeEvent =>
      val edgeChange: Option[(EdgeAction, HalfEdge)] = nodeEvent.event match {
        case EdgeAdded(halfEdge) => Some(EdgeAction.Added -> halfEdge)
        case EdgeRemoved(halfEdge) => Some(EdgeAction.Removed -> halfEdge)
        case _: PropertyEvent => None
        case _: DomainIndexEvent => None
      }
      edgeChange.flatMap { case (action, halfEdge) =>
        selfLoopKeyOf(halfEdge, node, edgeTypeFilter, directionFilter).map { case (key, side) =>
          HalfEdgeChange(action, key, nodeEvent.atTime.millis, side)
        }
      }.toList
    }

  /** The edge one of a self loop's half edges belongs to, and which half of it that is. */
  private def selfLoopKeyOf(
    halfEdge: HalfEdge,
    node: QuineId,
    edgeTypeFilter: Option[Set[Symbol]],
    directionFilter: Option[EdgeDirection],
  ): Option[(EdgeKey, HalfEdgeSide)] =
    if (halfEdge.other != node || !edgeTypeFilter.forall(_.contains(halfEdge.edgeType))) None
    else {
      // A loop runs from the node to itself, so `Outgoing` is the spelling kept for the directed
      // one; reporting it also as incoming would report one edge twice.
      val (direction, side) = halfEdge.direction match {
        case EdgeDirection.Outgoing => (EdgeDirection.Outgoing, HalfEdgeSide.Source)
        case EdgeDirection.Incoming => (EdgeDirection.Outgoing, HalfEdgeSide.Target)
        case EdgeDirection.Undirected => (EdgeDirection.Undirected, HalfEdgeSide.Whole)
      }
      Option.when(directionFilter.forall(_ == direction))((EdgeKey(halfEdge.edgeType, direction), side))
    }

  /** Collect, from one endpoint's journal, the changes to the half edge that endpoint holds for
    * edges running from the source node to the target node.
    *
    * @param journal journal of the node on `side` of the edge
    * @param otherEndpoint the node at the opposite end, which the half edge must point at
    * @param edgeTypeFilter the edge types to keep, any one of which matches, or `None` to keep every type
    * @param side which half of the edge this journal holds
    */
  private def halfEdgeChangesIn(
    journal: Source[NodeEvent.WithTime[NodeEvent], NotUsed],
    otherEndpoint: QuineId,
    edgeTypeFilter: Option[Set[Symbol]],
    directionFilter: Option[EdgeDirection],
    side: HalfEdgeSide,
  ): Source[HalfEdgeChange, NotUsed] =
    journal.mapConcat { nodeEvent =>
      val edgeChange: Option[(EdgeAction, HalfEdge)] = nodeEvent.event match {
        case EdgeAdded(halfEdge) => Some(EdgeAction.Added -> halfEdge)
        case EdgeRemoved(halfEdge) => Some(EdgeAction.Removed -> halfEdge)
        case _: PropertyEvent => None
        case _: DomainIndexEvent => None
      }
      edgeChange.flatMap { case (action, halfEdge) =>
        edgeKeyOf(halfEdge, otherEndpoint, edgeTypeFilter, directionFilter, side)
          .map(key => HalfEdgeChange(action, key, nodeEvent.atTime.millis, side))
      }.toList
    }

  /** The edge a half edge belongs to, if it is the `side` half of an edge running source -> target.
    *
    * A whole edge is stored as two half edges with opposite directions: the source node holds it as
    * `Outgoing` and the target node as `Incoming`, or both hold it as `Undirected`. A half edge
    * carrying the wrong direction for the side that holds it is therefore part of the edge running
    * the other way, which has its own lifecycle and is not what this call reports on.
    */
  private def edgeKeyOf(
    halfEdge: HalfEdge,
    otherEndpoint: QuineId,
    edgeTypeFilter: Option[Set[Symbol]],
    directionFilter: Option[EdgeDirection],
    side: HalfEdgeSide,
  ): Option[EdgeKey] =
    if (halfEdge.other != otherEndpoint || !edgeTypeFilter.forall(_.contains(halfEdge.edgeType))) None
    else {
      // The direction recorded is the one the queried node holds. Each node stores its own half
      // with the opposite direction, so the far node's half of an edge the queried node calls
      // outgoing is stored as incoming, and both map to the same edge.
      val asQueriedNodeHoldsIt = (halfEdge.direction, side) match {
        case (EdgeDirection.Outgoing, HalfEdgeSide.Source) => Some(EdgeDirection.Outgoing)
        case (EdgeDirection.Incoming, HalfEdgeSide.Target) => Some(EdgeDirection.Outgoing)
        case (EdgeDirection.Incoming, HalfEdgeSide.Source) => Some(EdgeDirection.Incoming)
        case (EdgeDirection.Outgoing, HalfEdgeSide.Target) => Some(EdgeDirection.Incoming)
        case (EdgeDirection.Undirected, HalfEdgeSide.Source) => Some(EdgeDirection.Undirected)
        case (EdgeDirection.Undirected, HalfEdgeSide.Target) => Some(EdgeDirection.Undirected)
        // `Whole` cannot reach here. It is only produced for an edge from a node to itself, which
        // is read from that one node's journal by `selfLoopChangesIn` rather than by pairing two.
        case (EdgeDirection.Outgoing, HalfEdgeSide.Whole) => None
        case (EdgeDirection.Incoming, HalfEdgeSide.Whole) => None
        case (EdgeDirection.Undirected, HalfEdgeSide.Whole) => None
      }
      asQueriedNodeHoldsIt
        .filter(direction => directionFilter.forall(_ == direction))
        .map(EdgeKey(halfEdge.edgeType, _))
    }
}
