package com.thatdot.quine.compiler.cypher

import scala.collection.immutable.{Map => ScalaMap}
import scala.concurrent.ExecutionContext

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.common.logging.Pretty.PrettyHelper
import com.thatdot.quine.compiler.cypher.HistoricalJournalSupport.{
  directionName,
  earlierOf,
  extractNode,
  measureRows,
  requireJournalEnabled,
}
import com.thatdot.quine.graph.LiteralOpsGraph
import com.thatdot.quine.graph.cypher._
import com.thatdot.quine.model.{Milliseconds, QuineIdProvider}

/** Run a read-only query as of a historical moment.
  *
  * A query's historical moment normally comes from the `at-time` request parameter, which pins the
  * whole query. This procedure moves that choice into the query itself, so one query can read the
  * graph as it stood at several different moments, and can compute the moment from data rather than
  * having to know it up front.
  *
  * == Signature ==
  * {{{
  * CALL history.queryAt(query, atTime, parameters)
  * YIELD value
  * }}}
  *
  * @param query The Cypher query to run, which must be read-only
  * @param atTime The moment to run the query at, in milliseconds since the Unix epoch
  * @param parameters Parameters for the query, or omitted for none
  * @return `value` - one map per row the query returned, keyed by the query's return columns
  *
  * == Usage Examples ==
  *
  * Read a node as it stood at a chosen moment:
  * {{{
  * CALL history.queryAt("MATCH (n:Account {id: 'A1'}) RETURN n.balance AS balance", 1693526400000)
  * YIELD value
  * RETURN value.balance
  * }}}
  *
  * Because the moment is an ordinary argument it can come from the graph, which lets one query
  * compare many moments — something the request parameter cannot express:
  * {{{
  * MATCH (audit:AuditPoint)
  * CALL history.queryAt(
  *   "MATCH (n:Account {id: $id}) RETURN n.balance AS balance",
  *   audit.timestamp,
  *   {id: audit.accountId}
  * )
  * YIELD value
  * RETURN audit.timestamp, value.balance
  * ORDER BY audit.timestamp
  * }}}
  *
  * == Implementation Notes ==
  * - The query is compiled and run once per invocation, so `atTime` may be computed per row
  * - The query must be read-only; writing to a historical moment is rejected rather than ignored
  * - The moment is used exactly as given. One that has not arrived yet is not rewritten to the
  *   present: a timestamp a moment ahead cannot be told apart from clock skew, so guessing which
  *   was meant would be wrong. It reads like the present anyway, in that only what has already
  *   happened can be reported
  * - When the query containing this call is itself pinned to a past moment by the `at-time`
  *   parameter, the earlier of the two moments is used. That query is a view of the graph as it
  *   stood then, and a procedure inside it that read further ahead would answer about that view's
  *   future
  * - Results are returned as a map per row rather than as columns, because the query's shape is
  *   not known until it is compiled
  * - Parameters are referenced as `$name` inside the query, as in any other Cypher query
  * - Requires the journal to be enabled; raises an error when it is not, because a node is rebuilt
  *   at a past moment by replaying its journal and without one the query would answer from the
  *   present while appearing to answer about the past
  */
object HistoricalQueryAt extends UserDefinedProcedure {
  val name = "history.queryAt"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = true

  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector(
      "query" -> Type.Str,
      "atTime" -> Type.Integer,
      "parameters" -> Type.Map,
    ),
    outputs = Vector("value" -> Type.Map),
    description = "Run a read-only Cypher query as of a historical moment",
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

    // Running a query at a past moment rebuilds each node it touches by replaying that node's
    // journal. Without one the query still runs and answers from whatever the present holds, which
    // is the one failure a query asked about the past must not have.
    requireJournalEnabled(LiteralOpsGraph.getOrThrow(s"`$name` procedure", location.graph), name)

    // Each argument is checked on its own so the error names the one at fault. Constraining them in
    // the pattern instead collapses a bad first or third argument into the same branch as a bad
    // second one, which can only report one of them.
    val (atTime, query, queryParameters) = arguments match {
      case Seq(queryLike, atTimeLike, rest @ _*) =>
        val queryText = queryLike match {
          case Expr.Str(q) => q
          case other =>
            throw CypherException.Runtime(
              s"`$name` expects a query string as the first argument, but got: $other",
            )
        }
        val moment = atTimeLike match {
          // Never later than the query containing this one: running the inner query further ahead
          // would answer a question about the future of the moment the outer query is pinned to.
          case Expr.Integer(millis) => earlierOf(Milliseconds(millis), location.atTime)
          case other =>
            throw CypherException.Runtime(
              s"`$name` expects an integer millisecond timestamp as the second argument, but got: $other",
            )
        }
        val queryParams = rest match {
          case Seq() => ScalaMap.empty[String, Value]
          case Seq(Expr.Map(ps)) => ps
          case Seq(other) =>
            throw CypherException.Runtime(
              s"`$name` expects a map of query parameters as the third argument, but got: $other",
            )
          case _ => throw wrongSignature(arguments)
        }
        (Some(moment), queryText, queryParams)
      case other => throw wrongSignature(other)
    }

    val subQueryResults = queryCypherValues(
      query,
      location.namespace,
      parameters = queryParameters,
      initialColumns = queryParameters,
      atTime = atTime,
    )(location.graph)

    if (!subQueryResults.compiled.isReadOnly)
      throw CypherException.Runtime(
        s"`$name` runs its query at a moment that has already passed, so the query must be read-only, " +
        s"but it writes to the graph: $query",
      )

    subQueryResults.results
      .map { (row: Vector[Value]) =>
        Vector(Expr.Map(subQueryResults.columns.map(_.name).zip(row.view)))
      }
      .via(measureRows(location.graph.metrics.stateReadMetrics(name)))
  }
}

/** Read a node as it stood at a historical moment.
  *
  * Where [[HistoricalQueryAt]] runs a whole query at a moment, this answers the narrower and much more
  * common question of what one node looked like then, without the query having to be written as a
  * string.
  *
  * == Signature ==
  * {{{
  * CALL history.nodeAt(node, atTime)
  * YIELD node
  * }}}
  *
  * @param node The node to read (can be a node or node ID)
  * @param atTime The moment to read it at, in milliseconds since the Unix epoch
  * @return `node` - the node with the properties and labels it had at that moment
  * @return `edges` - the half edges it held then, each as `{edgeType, other, direction}`
  *
  * == Usage Examples ==
  *
  * {{{
  * MATCH (n:Account {id: "A1"})
  * CALL history.nodeAt(n, 1693526400000) YIELD node AS past, edges AS edgesThen
  * RETURN past.balance AS balanceThen, n.balance AS balanceNow, size(edgesThen) AS edgeCountThen
  * }}}
  *
  * The edges are the halves the node held, so they can be read like any other list of maps:
  * {{{
  * MATCH (n:Account {id: "A1"})
  * CALL history.nodeAt(n, 1693526400000) YIELD edges
  * UNWIND edges AS edge
  * WITH edge WHERE edge.direction = "Outgoing"
  * RETURN edge.edgeType, edge.other
  * }}}
  *
  * Comparing a node against itself at several moments, taking the moments from the graph:
  * {{{
  * MATCH (n:Account {id: "A1"}), (audit:AuditPoint)
  * CALL history.nodeAt(n, audit.timestamp) YIELD node AS past
  * RETURN audit.timestamp, past.balance
  * ORDER BY audit.timestamp
  * }}}
  *
  * == Implementation Notes ==
  * - Reports the whole node as it stood: its properties, its labels, and the half edges it held
  * - The edges are what this node recorded, not checked against the nodes at the far end, so one
  *   listed here is not necessarily one `MATCH` would have traversed then; `history.edgeChangesBetween`
  *   answers that
  * - The moment is used exactly as given, and is combined with the containing query's own moment by
  *   taking the earlier of the two, both for the reasons given on [[HistoricalQueryAt]]
  * - Reports state rather than change, but still needs the journal: a node is rebuilt at a past
  *   moment by taking its most recent snapshot and replaying the journal from there, so without the
  *   journal only whatever a snapshot happened to capture could be reported
  */
object HistoricalNodeAt extends UserDefinedProcedure {
  val name = "history.nodeAt"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false

  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector(
      "node" -> Type.Node,
      "atTime" -> Type.Integer,
    ),
    outputs = Vector(
      "node" -> Type.Node,
      "edges" -> Type.ListOfAnything,
    ),
    description = "Read a node as it stood at a historical moment, with its properties, labels, and edges",
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
    // Rebuilding a node at a past moment replays its journal from the most recent snapshot, so
    // without the journal this could only report whatever a snapshot happened to capture.
    requireJournalEnabled(graph, name)
    implicit val idProvider: QuineIdProvider = graph.idProvider

    val (nodeId, atTime) = arguments match {
      case Seq(nodeLike, Expr.Integer(millis)) =>
        // Never later than the query containing this call: reading the node further ahead would put
        // state from after the outer query's moment into a view of how the graph stood then.
        (extractNode(nodeLike, name, "first"), Some(earlierOf(Milliseconds(millis), location.atTime)))
      case Seq(_, other) =>
        throw CypherException.Runtime(
          s"`$name` expects an integer millisecond timestamp as the second argument, but got: $other",
        )
      case other => throw wrongSignature(other)
    }

    implicit val ec: ExecutionContext = location.graph.nodeDispatcherEC
    val literalOps = graph.literalOps(location.namespace)

    // TODO: this asks the node twice — once for its properties and labels, once for its edges — so
    // a node on another member costs two round trips where one would do. Both go to the same
    // historical node, which replays its journal once on wakeup and answers the second from memory,
    // so the cost is network traffic rather than storage reads. A single message returning
    // properties, labels, and edges together would remove it. `GetPropertiesAndEdges` is close but
    // streams properties raw, leaving the labels property undecoded, so it would need to split
    // labels the way `GetPropertiesCommand` already does.
    Source
      .lazyFuture { () =>
        for {
          node <- UserDefinedProcedure.getAsCypherNode(nodeId, location.namespace, atTime, graph)
          halfEdges <- literalOps.getHalfEdges(nodeId, atTime = atTime)
        } yield {
          val edges = halfEdges.toVector
            .sortBy(halfEdge => (halfEdge.edgeType.name, halfEdge.other.pretty, directionName(halfEdge.direction)))
            .map { halfEdge =>
              Expr.Map(
                Seq(
                  "edgeType" -> Expr.Str(halfEdge.edgeType.name),
                  "other" -> Expr.Str(halfEdge.other.pretty),
                  "direction" -> Expr.Str(directionName(halfEdge.direction)),
                ),
              ): Value
            }
          Vector(node: Value, Expr.List(edges))
        }
      }
      .mapMaterializedValue(_ => NotUsed)
      .via(measureRows(location.graph.metrics.stateReadMetrics(name)))
  }
}
