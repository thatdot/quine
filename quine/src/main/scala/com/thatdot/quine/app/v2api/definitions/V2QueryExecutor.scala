package com.thatdot.quine.app.v2api.definitions

import scala.concurrent.ExecutionContext

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import io.circe.Json

import com.thatdot.api.v2.QueryWebSocketProtocol.{UiEdge, UiNode}
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.routes.OSSQueryUiCypherMethods
import com.thatdot.quine.graph.{CypherOpsGraph, LiteralOpsGraph, NamespaceId}
import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.routes.{CypherQuery, UiEdge => V1UiEdge, UiNode => V1UiNode}

/** Separates protocol handling (serialization, query tracking, cancellation) from query execution (compilation, graph
  * access, ID conversion). Implementations bind to a specific namespace and convert results to V2 types.
  */
trait V2QueryExecutor {

  /** Execute a node query.
    *
    * @return (source of V2 UiNode results, isReadOnly, canContainAllNodeScan)
    */
  def executeNodeQuery(
    query: CypherQuery,
    atTime: Option[Milliseconds],
    useQuinePattern: Boolean,
  ): (Source[UiNode, NotUsed], Boolean, Boolean)

  /** Execute an edge query.
    *
    * @return (source of V2 UiEdge results, isReadOnly, canContainAllNodeScan)
    */
  def executeEdgeQuery(
    query: CypherQuery,
    atTime: Option[Milliseconds],
    useQuinePattern: Boolean,
  ): (Source[UiEdge, NotUsed], Boolean, Boolean)

  /** Execute a text (tabular) query.
    *
    * @return (column names, source of result rows, isReadOnly, canContainAllNodeScan)
    */
  def executeTextQuery(
    query: CypherQuery,
    atTime: Option[Milliseconds],
    useQuinePattern: Boolean,
  ): (Seq[String], Source[Seq[Json], NotUsed], Boolean, Boolean)

  /** True if the underlying query executor (graph) is ready, otherwise false. */
  def isReady: Boolean

  /** Execution context for async stream lifecycle callbacks. */
  def executionContext: ExecutionContext
}

/** [[V2QueryExecutor]] backed by the OSS Cypher/QuinePattern query methods.
  *
  * Wraps [[OSSQueryUiCypherMethods]] and converts V1 result types (`UiNode[QuineId]`, `UiEdge[QuineId]`) to V2 string-ID
  * types.
  *
  * @param graph the graph instance for query execution
  * @param namespaceId namespace to run queries in (bound at construction time)
  */
class OSSQueryExecutor(
  graph: LiteralOpsGraph with CypherOpsGraph,
  namespaceId: NamespaceId,
)(implicit logConfig: LogConfig)
    extends V2QueryExecutor {

  private val cypherMethods = new OSSQueryUiCypherMethods(graph)
  private val idToString: QuineId => String = graph.idProvider.qidToPrettyString

  private def toV2Node(n: V1UiNode[QuineId]): UiNode =
    UiNode(idToString(n.id), n.hostIndex, n.label, n.properties)

  private def toV2Edge(e: V1UiEdge[QuineId]): UiEdge =
    UiEdge(idToString(e.from), e.edgeType, idToString(e.to), e.isDirected)

  def executeNodeQuery(
    query: CypherQuery,
    atTime: Option[Milliseconds],
    useQuinePattern: Boolean,
  ): (Source[UiNode, NotUsed], Boolean, Boolean) = {
    val (source, ro, scan) =
      if (useQuinePattern) cypherMethods.quinePatternQueryNodes(query, namespaceId, atTime)
      else cypherMethods.queryCypherNodes(query, namespaceId, atTime)
    (source.map(toV2Node), ro, scan)
  }

  def executeEdgeQuery(
    query: CypherQuery,
    atTime: Option[Milliseconds],
    useQuinePattern: Boolean,
  ): (Source[UiEdge, NotUsed], Boolean, Boolean) = {
    val (source, ro, scan) =
      if (useQuinePattern) cypherMethods.quinePatternQueryEdges(query, namespaceId, atTime)
      else cypherMethods.queryCypherEdges(query, namespaceId, atTime)
    (source.map(toV2Edge), ro, scan)
  }

  def executeTextQuery(
    query: CypherQuery,
    atTime: Option[Milliseconds],
    useQuinePattern: Boolean,
  ): (Seq[String], Source[Seq[Json], NotUsed], Boolean, Boolean) = {
    val (cols, source, ro, scan) =
      if (useQuinePattern) cypherMethods.quinePatternQueryGeneric(query, namespaceId, atTime)
      else cypherMethods.queryCypherGeneric(query, namespaceId, atTime)
    (cols, source, ro, scan)
  }

  def isReady: Boolean = graph.isReady

  def executionContext: ExecutionContext = graph.shardDispatcherEC
}
