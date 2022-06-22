package com.thatdot.quine.app.routes

import scala.concurrent.{ExecutionContext, Future}

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.{ByteString, Timeout}

import com.thatdot.quine.graph._
import com.thatdot.quine.graph.messaging.LiteralMessage.{
  DgbLocalEventIndexSummary,
  LocallyRegisteredStandingQuery,
  NodeInternalState,
  SqStateResult,
  SqStateResults
}
import com.thatdot.quine.model
import com.thatdot.quine.model.{EdgeDirection => _, _}
import com.thatdot.quine.routes.EdgeDirection._
import com.thatdot.quine.routes._

/** The Akka HTTP implementation of [[LiteralRoutes]] */
trait LiteralRoutesImpl
    extends LiteralRoutes
    with endpoints4s.akkahttp.server.Endpoints
    with endpoints4s.akkahttp.server.JsonEntitiesFromSchemas
    with exts.ServerQuineEndpoints {

  implicit def toEdgeDirection(dir: model.EdgeDirection): EdgeDirection = dir match {
    case model.EdgeDirection.Outgoing => Outgoing
    case model.EdgeDirection.Incoming => Incoming
    case model.EdgeDirection.Undirected => Undirected
  }

  implicit def fromEdgeDirection(dir: EdgeDirection): model.EdgeDirection = dir match {
    case Outgoing => model.EdgeDirection.Outgoing
    case Incoming => model.EdgeDirection.Incoming
    case Undirected => model.EdgeDirection.Undirected
  }

  /* Not implicit since we use this only _explicitly_ to turn [[NodeInternalState]]
   * into JSON (the choice not to expose a JSON schema for the endpoint is
   * intentional, so as to discourage users from using this outside of debugging)
   */
  lazy val nodeInternalStateSchema: JsonSchema[NodeInternalState] = {
    implicit val quineValueSchema: JsonSchema[QuineValue] =
      anySchema(None).xmap(QuineValue.fromJson)(QuineValue.toJson)
    implicit val propertyValueSchema: JsonSchema[PropertyValue] =
      quineValueSchema.xmap(PropertyValue.apply)(_.deserialized.get)
    implicit val halfEdgeSchema: JsonSchema[HalfEdge] = genericJsonSchema[HalfEdge]
    implicit val lSq: JsonSchema[LocallyRegisteredStandingQuery] =
      genericJsonSchema[LocallyRegisteredStandingQuery]
    implicit val eventTimeSchema: JsonSchema[EventTime] =
      longJsonSchema.xmap(EventTime.fromRaw)(_.eventTime)
    implicit val dgbLocalEventIndexSummarySchema: JsonSchema[DgbLocalEventIndexSummary] =
      genericJsonSchema[DgbLocalEventIndexSummary]
    implicit val nchSchema: JsonSchema[NodeChangeEvent] = genericJsonSchema[NodeChangeEvent]
    implicit val nchwtSchema: JsonSchema[NodeChangeEvent.WithTime] = genericJsonSchema[NodeChangeEvent.WithTime]
    implicit val sqResult: JsonSchema[SqStateResult] = genericJsonSchema[SqStateResult]
    implicit val sqResults: JsonSchema[SqStateResults] = genericJsonSchema[SqStateResults]
    genericJsonSchema[NodeInternalState]
  }

  implicit def graph: LiteralOpsGraph
  implicit def timeout: Timeout
  implicit def ec: ExecutionContext

  private val literalGetRoute = literalGet.implementedByAsync { case (qid: QuineId, atTime: AtTime) =>
    val propsF = graph.literalOps.getProps(qid, atTime = atTime)
    val edgesF = graph.literalOps.getEdges(qid, atTime = atTime)
    for {
      props <- propsF
      edges <- edgesF
    } yield LiteralNode(
      props.map { case (k, v) => k.name -> ByteString(v.serialized) },
      edges.toSeq.map { case HalfEdge(t, d, o) => RestHalfEdge(t.name, d, o) }
    )
  }

  private val literalMergeNodesRoute = literalMergeNodes.implementedByAsync {
    case (intoThis: QuineId, fromThat: QuineId) => graph.literalOps.mergeNode(fromThat, intoThis)
  }

  private val literalPostRoute = literalPost.implementedByAsync {
    case (qid: QuineId, node: LiteralNode[QuineId, ByteString]) =>
      val propsF = Future.traverse(node.properties: TraversableOnce[(String, BStr)]) { case (typ, value) =>
        graph.literalOps.setPropBytes(qid, typ, value.toArray)
      }
      val edgesF = Future.traverse(node.edges) {
        case RestHalfEdge(typ, Outgoing, to) => graph.literalOps.addEdge(qid, to, typ, true)
        case RestHalfEdge(typ, Incoming, to) => graph.literalOps.addEdge(to, qid, typ, true)
        case RestHalfEdge(typ, Undirected, to) => graph.literalOps.addEdge(qid, to, typ, false)
      }
      for {
        props <- propsF
        edges <- edgesF
      } yield ()
  }

  private val literalDeleteRoute = literalDelete.implementedByAsync { (qid: QuineId) =>
    graph.literalOps.deleteNode(qid)
  }

  protected val literalDebugRoute: Route = literalDebug.implementedByAsync { case (qid: QuineId, atTime: AtTime) =>
    graph.literalOps.logState(qid, atTime).map(nodeInternalStateSchema.encoder.encode)
  }

  private val literalEdgesGetRoute = literalEdgesGet.implementedByAsync {
    case (qid, (atTime, limit, edgeDirOpt, otherOpt, edgeTypeOpt)) =>
      val edgeDirOpt2 = edgeDirOpt.map(fromEdgeDirection)
      graph.literalOps
        .getEdges(qid, edgeTypeOpt.map(Symbol.apply), edgeDirOpt2, otherOpt, limit, atTime)
        .map(_.toVector.map { case HalfEdge(t, d, o) => RestHalfEdge(t.name, d, o) })
  }

  private val literalEdgePutRoute = literalEdgePut.implementedByAsync { case (qid, edges) =>
    Future
      .traverse(edges) { case RestHalfEdge(edgeType, edgeDir, other) =>
        edgeDir match {
          case Undirected => graph.literalOps.addEdge(qid, other, edgeType, isDirected = false)
          case Outgoing => graph.literalOps.addEdge(qid, other, edgeType, isDirected = true)
          case Incoming => graph.literalOps.addEdge(other, qid, edgeType, isDirected = true)
        }
      }
      .map(_ => ())
  }

  private val literalEdgeDeleteRoute = literalEdgeDelete.implementedByAsync { case (qid, edges) =>
    Future
      .traverse(edges) { case RestHalfEdge(edgeType, edgeDir, other) =>
        edgeDir match {
          case Undirected =>
            graph.literalOps.removeEdge(qid, other, edgeType, isDirected = false)
          case Outgoing => graph.literalOps.removeEdge(qid, other, edgeType, isDirected = true)
          case Incoming => graph.literalOps.removeEdge(other, qid, edgeType, isDirected = true)
        }
      }
      .map(_ => ())
  }

  private val literalHalfEdgesGetRoute = literalHalfEdgesGet.implementedByAsync {
    case (qid, (atTime, limit, edgeDirOpt, otherOpt, edgeTypeOpt)) =>
      val edgeDirOpt2 = edgeDirOpt.map(fromEdgeDirection)
      graph.literalOps
        .getHalfEdges(qid, edgeTypeOpt.map(Symbol.apply), edgeDirOpt2, otherOpt, limit, atTime)
        .map(_.toVector.map { case HalfEdge(t, d, o) => RestHalfEdge(t.name, d, o) })
  }

  private val literalPropertyGetRoute = literalPropertyGet.implementedByAsync { case (qid, propKey, atTime) =>
    graph.literalOps
      .getProps(qid, atTime)
      .map(m => m.get(Symbol(propKey)).map(_.serialized).map(ByteString(_)))
  }

  private val literalPropertyPutRoute = literalPropertyPut.implementedByAsync { case (qid, propKey, value) =>
    graph.literalOps
      .setPropBytes(qid, propKey, value.toArray)
  }

  private val literalPropertyDeleteRoute = literalPropertyDelete.implementedByAsync { case (qid, propKey) =>
    graph.literalOps
      .removeProp(qid, propKey)
  }

  final val literalRoutes: Route = {
    literalGetRoute ~
    literalMergeNodesRoute ~ // QU-353
    literalDeleteRoute ~
    literalPostRoute ~
    literalDebugRoute ~
    literalEdgesGetRoute ~
    literalEdgePutRoute ~
    literalEdgeDeleteRoute ~
    literalHalfEdgesGetRoute ~
    literalPropertyGetRoute ~
    literalPropertyPutRoute ~
    literalPropertyDeleteRoute
  }
}
