package com.thatdot.quine.app.routes

import scala.concurrent.duration.Duration

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import io.circe.Json

import com.thatdot.quine.compiler.cypher
import com.thatdot.quine.graph.cypher.{
  CypherException,
  Expr => CypherExpr,
  RunningCypherQuery => CypherRunningQuery,
  Type => CypherType,
  Value => CypherValue
}
import com.thatdot.quine.graph.{CypherOpsGraph, LiteralOpsGraph, NamespaceId}
import com.thatdot.quine.model._
import com.thatdot.quine.routes._
import com.thatdot.quine.util.Log._

trait QueryUiCypherApiMethods extends LazySafeLogging {
  implicit def graph: LiteralOpsGraph with CypherOpsGraph
  implicit def idProvider: QuineIdProvider
  implicit protected def logConfig: LogConfig

  /** Compute the host of a quine ID */
  def hostIndex(qid: QuineId): Int
  private def guessCypherParameters(params: Map[String, Json]): Map[String, CypherValue] =
    params.map { case (k, v) => k -> CypherExpr.fromQuineValue(QuineValue.fromJson(v)) }

  /** Post-process UI nodes. This serves as a hook for last minute modifications to the nodes sen
    * out to the UI.
    *
    * @param uiNode UI node to modify
    * @return updated UI node
    */
  protected def transformUiNode(uiNode: UiNode[QuineId]): UiNode[QuineId]

  /** Query nodes with a given Cypher query
    *
    * @note this filters out nodes whose IDs are not supported by the provider
    *
    * @param query Cypher query expected to return nodes
    * @param namespace Which namespace to query in.
    * @param atTime possibly historical time to query
    * @return tuple of nodes produced by the query, whether the query is read-only, and whether the query may cause full node scan
    */
  final def queryCypherNodes(
    query: CypherQuery,
    namespace: NamespaceId,
    atTime: Option[Milliseconds]
  ): (Source[UiNode[QuineId], NotUsed], Boolean, Boolean) = {
    val res: CypherRunningQuery = cypher.queryCypherValues(
      query.text,
      parameters = guessCypherParameters(query.parameters),
      namespace = namespace,
      atTime = atTime
    )

    val results = res.results
      .mapConcat(identity)
      .map[UiNode[QuineId]] {
        case CypherExpr.Node(qid, labels, properties) =>
          val nodeLabel = if (labels.nonEmpty) {
            labels.map(_.name).mkString(":")
          } else {
            "ID: " + qid.pretty
          }

          UiNode(
            id = qid,
            hostIndex = hostIndex(qid),
            label = nodeLabel,
            properties = properties.map { case (k, v) => (k.name, CypherValue.toJson(v)) }
          )

        case other =>
          throw CypherException.TypeMismatch(
            expected = Seq(CypherType.Node),
            actualValue = other,
            context = "node query return value"
          )
      }
      .map(transformUiNode)

    (results, res.compiled.isReadOnly, res.compiled.canContainAllNodeScan)
  }

  /** Query edges with a given Cypher query
    *
    * @note this filters out nodes whose IDs are not supported by the provider
    *
    * @param query Cypher query expected to return edges
    * @param namespace the namespace in which to run this query
    * @param atTime possibly historical time to query
    * @param requestTimeout timeout signalling output results no longer matter
    * @return tuple of edges produced by the query, readonly, and canContainAllNodeScan
    */
  def queryCypherEdges(
    query: CypherQuery,
    namespace: NamespaceId,
    atTime: Option[Milliseconds],
    requestTimeout: Duration = Duration.Inf
  ): (Source[UiEdge[QuineId], NotUsed], Boolean, Boolean) = {
    val res: CypherRunningQuery = cypher.queryCypherValues(
      query.text,
      parameters = guessCypherParameters(query.parameters),
      namespace = namespace,
      atTime = atTime
    )

    val results = res.results
      .mapConcat(identity)
      .map[UiEdge[QuineId]] {
        case CypherExpr.Relationship(src, lbl, _, tgt) =>
          UiEdge(from = src, to = tgt, edgeType = lbl.name)

        case other =>
          throw CypherException.TypeMismatch(
            expected = Seq(CypherType.Relationship),
            actualValue = other,
            context = "edge query return value"
          )
      }

    (results, res.compiled.isReadOnly, res.compiled.canContainAllNodeScan)
  }

  /** Query anything with a given cypher query
    *
    * @note queries starting with `EXPLAIN` are intercepted (since they are
    * anyways not valid Cypher) and return one value which represents the
    * execution plan of the query without running the query.
    *
    * @param query Cypher query
    * @param namespace the namespace in which to run this query
    * @param atTime possibly historical time to query
    * @return data produced by the query formatted as JSON
    */
  def queryCypherGeneric(
    query: CypherQuery,
    namespace: NamespaceId,
    atTime: Option[Milliseconds]
  ): (Seq[String], Source[Seq[Json], NotUsed], Boolean, Boolean) = {

    // TODO: remove `PROFILE` here too
    val ExplainedQuery = raw"(?is)\s*explain\s+(.*)".r
    val (explainQuery, queryText) = query.text match {
      case ExplainedQuery(toExplain) => true -> toExplain
      case other => false -> other
    }

    val res: CypherRunningQuery = cypher.queryCypherValues(
      queryText,
      parameters = guessCypherParameters(query.parameters),
      namespace = namespace,
      atTime = atTime
    )

    if (!explainQuery) {
      val columns = res.columns.map(_.name)
      val bodyRows = res.results.map(row => row.map(CypherValue.toJson))
      (columns, bodyRows, res.compiled.isReadOnly, res.compiled.canContainAllNodeScan)
    } else {
      logger.debug(log"User requested EXPLAIN of query: ${res.compiled.query.toString}")
      val plan = cypher.Plan.fromQuery(res.compiled.query).toValue
      (Vector("plan"), Source.single(Seq(CypherValue.toJson(plan))), true, false)
    }
  }

}

class OSSQueryUiCypherMethods(quineGraph: LiteralOpsGraph with CypherOpsGraph)(implicit
  protected val logConfig: LogConfig
) extends QueryUiCypherApiMethods() {
  def hostIndex(qid: com.thatdot.quine.model.QuineId): Int = 0
  override def idProvider: QuineIdProvider = graph.idProvider
  def transformUiNode(uiNode: com.thatdot.quine.routes.UiNode[com.thatdot.quine.model.QuineId]) = uiNode
  override def graph = quineGraph
}
