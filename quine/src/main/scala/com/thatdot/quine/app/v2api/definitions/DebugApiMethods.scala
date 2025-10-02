package com.thatdot.quine.app.v2api.definitions

import scala.concurrent.Future

import org.apache.pekko.util.Timeout

import io.circe.Json

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.v2api.endpoints.V2DebugEndpointEntities.{TEdgeDirection, TLiteralNode, TRestHalfEdge}
import com.thatdot.quine.graph.{BaseGraph, CypherOpsGraph, LiteralOpsGraph, NamespaceId}
import com.thatdot.quine.model
import com.thatdot.quine.model.{HalfEdge, Milliseconds, QuineValue}

trait DebugApiMethods {
  val graph: BaseGraph with LiteralOpsGraph with CypherOpsGraph
  implicit val logConfig: LogConfig
  implicit def timeout: Timeout

  private def toApiEdgeDirection(dir: model.EdgeDirection): TEdgeDirection = dir match {
    case model.EdgeDirection.Outgoing => TEdgeDirection.Outgoing
    case model.EdgeDirection.Incoming => TEdgeDirection.Incoming
    case model.EdgeDirection.Undirected => TEdgeDirection.Undirected
  }

  private def toModelEdgeDirection(dir: TEdgeDirection): model.EdgeDirection = dir match {
    case TEdgeDirection.Outgoing => model.EdgeDirection.Outgoing
    case TEdgeDirection.Incoming => model.EdgeDirection.Incoming
    case TEdgeDirection.Undirected => model.EdgeDirection.Undirected
  }

  def debugOpsPropertyGet(
    qid: QuineId,
    propKey: String,
    atTime: Option[Milliseconds],
    namespaceId: NamespaceId,
  ): Future[Option[Json]] =
    graph.requiredGraphIsReadyFuture {
      graph
        .literalOps(namespaceId)
        .getProps(qid, atTime)
        .map(m =>
          m.get(Symbol(propKey))
            .map(_.deserialized.get)
            .map(qv => QuineValue.toJson(qv)(graph.idProvider, logConfig)),
        )(
          graph.nodeDispatcherEC,
        )
    }

  def debugOpsGet(qid: QuineId, atTime: Option[Milliseconds], namespaceId: NamespaceId): Future[TLiteralNode[QuineId]] =
    graph.requiredGraphIsReadyFuture {
      val propsF = graph.literalOps(namespaceId).getProps(qid, atTime = atTime)
      val edgesF = graph.literalOps(namespaceId).getEdges(qid, atTime = atTime)
      propsF
        .zip(edgesF)
        .map { case (props, edges) =>
          TLiteralNode(
            props.map { case (k, v) =>
              k.name -> QuineValue.toJson(v.deserialized.get)(graph.idProvider, logConfig)
            },
            edges.toSeq.map { case HalfEdge(t, d, o) => TRestHalfEdge(t.name, toApiEdgeDirection(d), o) },
          )
        }(graph.nodeDispatcherEC)
    }

  def debugOpsVerbose(qid: QuineId, atTime: Option[Milliseconds], namespaceId: NamespaceId): Future[String] =
    graph.requiredGraphIsReadyFuture {
      graph
        .literalOps(namespaceId)
        .logState(qid, atTime)
        //TODO: ToString -> see DebugOpsRoutes.nodeInternalStateSchema
        .map(_.toString)(graph.nodeDispatcherEC)
    }

  def debugOpsEdgesGet(
    qid: QuineId,
    atTime: Option[Milliseconds],
    limit: Option[Int],
    edgeDirOpt: Option[TEdgeDirection],
    otherOpt: Option[QuineId],
    edgeTypeOpt: Option[String],
    namespaceId: NamespaceId,
  ): Future[Vector[TRestHalfEdge[QuineId]]] =
    graph.requiredGraphIsReadyFuture {
      val edgeDirOpt2 = edgeDirOpt.map(toModelEdgeDirection)
      graph
        .literalOps(namespaceId)
        .getEdges(qid, edgeTypeOpt.map(Symbol.apply), edgeDirOpt2, otherOpt, limit, atTime)
        .map(_.toVector.map { case HalfEdge(t, d, o) => TRestHalfEdge(t.name, toApiEdgeDirection(d), o) })(
          graph.nodeDispatcherEC,
        )

    }

  def debugOpsHalfEdgesGet(
    qid: QuineId,
    atTime: Option[Milliseconds],
    limit: Option[Int],
    edgeDirOpt: Option[TEdgeDirection],
    otherOpt: Option[QuineId],
    edgeTypeOpt: Option[String],
    namespaceId: NamespaceId,
  ): Future[Vector[TRestHalfEdge[QuineId]]] =
    graph.requiredGraphIsReadyFuture {
      val edgeDirOpt2 = edgeDirOpt.map(toModelEdgeDirection)
      graph
        .literalOps(namespaceId)
        .getHalfEdges(qid, edgeTypeOpt.map(Symbol.apply), edgeDirOpt2, otherOpt, limit, atTime)
        .map(_.toVector.map { case HalfEdge(t, d, o) => TRestHalfEdge(t.name, toApiEdgeDirection(d), o) })(
          graph.nodeDispatcherEC,
        )
    }
}
