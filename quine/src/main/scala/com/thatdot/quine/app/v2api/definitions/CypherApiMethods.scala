package com.thatdot.quine.app.v2api.definitions

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import org.apache.pekko.stream.scaladsl.Sink

import com.thatdot.api.v2.ErrorResponse.BadRequest
import com.thatdot.api.v2.ErrorType
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.routes.{OSSQueryUiCypherMethods, Util}
import com.thatdot.quine.app.v2api.endpoints.V2CypherEndpointEntities.{
  TCypherQuery,
  TCypherQueryResult,
  TUiEdge,
  TUiNode,
}
import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.graph.{BaseGraph, CypherOpsGraph, LiteralOpsGraph, NamespaceId}
import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.{routes => V1}

trait CypherApiMethods {
  val graph: BaseGraph with CypherOpsGraph with LiteralOpsGraph
  implicit val logConfig: LogConfig

  // The Query UI relies heavily on a couple Cypher endpoints for making queries.
  private def catchCypherException[A](futA: => Future[A]): Future[Either[BadRequest, A]] =
    Future
      .fromTry(Try(futA))
      .flatten
      .transform {
        case Success(a) => Success(Right(a))
        case Failure(qce: CypherException) => Success(Left(BadRequest(ErrorType.CypherError(qce.pretty))))
        case Failure(err) => Failure(err)
      }(ExecutionContext.parasitic)

  //TODO On missing namespace
  //TODO timeout handling
  val cypherMethods = new OSSQueryUiCypherMethods(graph)

  def cypherPost(
    atTime: Option[Milliseconds],
    timeout: FiniteDuration,
    namespaceId: NamespaceId,
    query: TCypherQuery,
  ): Future[Either[BadRequest, TCypherQueryResult]] =
    graph.requiredGraphIsReadyFuture {
      catchCypherException {
        val (columns, results, isReadOnly, _) =
          cypherMethods.queryCypherGeneric(
            V1.CypherQuery(query.text, query.parameters),
            namespaceId,
            atTime,
          ) // TODO read canContainAllNodeScan
        results
          .via(Util.completionTimeoutOpt(timeout, allowTimeout = isReadOnly))
          .named(s"cypher-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .runWith(Sink.seq)(graph.materializer)
          .map(TCypherQueryResult(columns, _))(ExecutionContext.parasitic)
      }
    }

  def cypherNodesPost(
    atTime: Option[Milliseconds],
    timeout: FiniteDuration,
    namespaceId: NamespaceId,
    query: TCypherQuery,
  ): Future[Either[BadRequest, Seq[TUiNode]]] =
    graph.requiredGraphIsReadyFuture {
      catchCypherException {
        val (results, isReadOnly, _) =
          cypherMethods.queryCypherNodes(
            V1.CypherQuery(query.text, query.parameters),
            namespaceId,
            atTime,
          ) // TODO read canContainAllNodeScan
        results
          .via(Util.completionTimeoutOpt(timeout, allowTimeout = isReadOnly))
          .named(s"cypher-nodes-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .map(node => TUiNode(node.id, node.hostIndex, node.label, node.properties))
          .runWith(Sink.seq)(graph.materializer)
      }
    }

  def cypherEdgesPost(
    atTime: Option[Milliseconds],
    timeout: FiniteDuration,
    namespaceId: NamespaceId,
    query: TCypherQuery,
  ): Future[Either[BadRequest, Seq[TUiEdge]]] =
    graph.requiredGraphIsReadyFuture {
      catchCypherException {
        val (results, isReadOnly, _) =
          cypherMethods.queryCypherEdges(
            V1.CypherQuery(query.text, query.parameters),
            namespaceId,
            atTime,
          ) // TODO read canContainAllNodeScan
        results
          .via(Util.completionTimeoutOpt(timeout, allowTimeout = isReadOnly))
          .named(s"cypher-edges-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .map(edge => TUiEdge(edge.from, edge.edgeType, edge.to, edge.isDirected))
          .runWith(Sink.seq)(graph.materializer)
      }
    }

}
