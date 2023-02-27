package com.thatdot.quine.app.routes

import scala.compat.ExecutionContexts
import scala.concurrent.Future

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout

import io.circe.Json

import com.thatdot.quine.graph._
import com.thatdot.quine.graph.messaging.LiteralMessage.{
  DgnLocalEventIndexSummary,
  LocallyRegisteredStandingQuery,
  NodeInternalState,
  SqStateResult,
  SqStateResults
}
import com.thatdot.quine.model
import com.thatdot.quine.model.{EdgeDirection => _, _}
import com.thatdot.quine.routes.EdgeDirection._
import com.thatdot.quine.routes._

/** The Akka HTTP implementation of [[DebugOpsRoutes]] */
trait DebugRoutesImpl
    extends DebugOpsRoutes
    with endpoints4s.akkahttp.server.Endpoints
    with exts.circe.JsonEntitiesFromSchemas
    with exts.ServerQuineEndpoints {

  private def toEdgeDirection(dir: model.EdgeDirection): EdgeDirection = dir match {
    case model.EdgeDirection.Outgoing => Outgoing
    case model.EdgeDirection.Incoming => Incoming
    case model.EdgeDirection.Undirected => Undirected
  }

  private def fromEdgeDirection(dir: EdgeDirection): model.EdgeDirection = dir match {
    case Outgoing => model.EdgeDirection.Outgoing
    case Incoming => model.EdgeDirection.Incoming
    case Undirected => model.EdgeDirection.Undirected
  }

  /* Not implicit since we use this only _explicitly_ to turn [[NodeInternalState]]
   * into JSON (the choice not to expose a JSON schema for the endpoint is
   * intentional, so as to discourage users from using this outside of debugging)
   *
   * TODO this should be possible to rewrite as just "define a schema for quinevalue, propertyvalue, and eventtime, then
   *    derive the rest" -- The implicit resolution scope will need to be corrected but we could remove the redundant
   *    intermediate implicits.
   */
  lazy val nodeInternalStateSchema: Record[NodeInternalState] = {
    implicit val quineValueSchema: JsonSchema[QuineValue] =
      anySchema(None).xmap(QuineValue.fromJson)(QuineValue.toJson)
    implicit val propertyValueSchema: JsonSchema[PropertyValue] =
      quineValueSchema.xmap(PropertyValue.apply)(_.deserialized.get)
    implicit val eventTimeSchema: JsonSchema[EventTime] =
      longJsonSchema.xmap(EventTime.fromRaw)(_.eventTime)
    implicit val msSchema: Record[Milliseconds] =
      genericRecord[Milliseconds]
    implicit val halfEdgeSchema: Record[HalfEdge] = genericRecord[HalfEdge]
    implicit val lSq: Record[LocallyRegisteredStandingQuery] =
      genericRecord[LocallyRegisteredStandingQuery]
    implicit val sqIdSchema: Record[StandingQueryId] = genericRecord[StandingQueryId]
    implicit val dgnLocalEventIndexSummarySchema: Record[DgnLocalEventIndexSummary] =
      genericRecord[DgnLocalEventIndexSummary]
    implicit val neSchema: Tagged[NodeEvent] = genericTagged[NodeEvent]
    implicit val newtSchema: Record[NodeEvent.WithTime[NodeEvent]] = genericRecord[NodeEvent.WithTime[NodeEvent]]
    implicit val sqResult: Record[SqStateResult] = genericRecord[SqStateResult]
    implicit val sqResults: Record[SqStateResults] = genericRecord[SqStateResults]
    genericRecord[NodeInternalState]
  }

  implicit def graph: LiteralOpsGraph
  implicit def timeout: Timeout

  private val debugGetRoute = debugOpsGet.implementedByAsync { case (qid: QuineId, atTime: AtTime) =>
    val propsF = graph.literalOps.getProps(qid, atTime = atTime)
    val edgesF = graph.literalOps.getEdges(qid, atTime = atTime)
    propsF
      .zip(edgesF)
      .map { case (props, edges) =>
        LiteralNode(
          props.map { case (k, v) => k.name -> QuineValue.toJson(v.deserialized.get)(graph.idProvider) },
          edges.toSeq.map { case HalfEdge(t, d, o) => RestHalfEdge(t.name, toEdgeDirection(d), o) }
        )
      }(graph.nodeDispatcherEC)
  }

  private val debugPostRoute = debugOpsPut.implementedByAsync { case (qid: QuineId, node: LiteralNode[QuineId]) =>
    val propsF = Future.traverse(node.properties: TraversableOnce[(String, Json)]) { case (typ, value) =>
      graph.literalOps.setProp(qid, typ, QuineValue.fromJson(value))
    }(implicitly, graph.nodeDispatcherEC)
    val edgesF = Future.traverse(node.edges) {
      case RestHalfEdge(typ, Outgoing, to) => graph.literalOps.addEdge(qid, to, typ, true)
      case RestHalfEdge(typ, Incoming, to) => graph.literalOps.addEdge(to, qid, typ, true)
      case RestHalfEdge(typ, Undirected, to) => graph.literalOps.addEdge(qid, to, typ, false)
    }(implicitly, graph.nodeDispatcherEC)
    propsF.flatMap(_ => edgesF)(ExecutionContexts.parasitic).map(_ => ())(ExecutionContexts.parasitic)
  }

  private val debugDeleteRoute = debugOpsDelete.implementedByAsync { (qid: QuineId) =>
    graph.literalOps.deleteNode(qid)
  }

  protected val debugVerboseRoute: Route = debugOpsVerbose.implementedByAsync { case (qid: QuineId, atTime: AtTime) =>
    graph.literalOps.logState(qid, atTime).map(nodeInternalStateSchema.encoder(_))(graph.nodeDispatcherEC)
  }

  private val debugEdgesGetRoute = debugOpsEdgesGet.implementedByAsync {
    case (qid, (atTime, limit, edgeDirOpt, otherOpt, edgeTypeOpt)) =>
      val edgeDirOpt2 = edgeDirOpt.map(fromEdgeDirection)
      graph.literalOps
        .getEdges(qid, edgeTypeOpt.map(Symbol.apply), edgeDirOpt2, otherOpt, limit, atTime)
        .map(_.toVector.map { case HalfEdge(t, d, o) => RestHalfEdge(t.name, toEdgeDirection(d), o) })(
          graph.nodeDispatcherEC
        )
  }

  private val debugEdgesPutRoute = debugOpsEdgesPut.implementedByAsync { case (qid, edges) =>
    Future
      .traverse(edges) { case RestHalfEdge(edgeType, edgeDir, other) =>
        edgeDir match {
          case Undirected => graph.literalOps.addEdge(qid, other, edgeType, isDirected = false)
          case Outgoing => graph.literalOps.addEdge(qid, other, edgeType, isDirected = true)
          case Incoming => graph.literalOps.addEdge(other, qid, edgeType, isDirected = true)
        }
      }(implicitly, graph.nodeDispatcherEC)
      .map(_ => ())(ExecutionContexts.parasitic)
  }

  private val debugEdgesDeleteRoute = debugOpsEdgeDelete.implementedByAsync { case (qid, edges) =>
    Future
      .traverse(edges) { case RestHalfEdge(edgeType, edgeDir, other) =>
        edgeDir match {
          case Undirected =>
            graph.literalOps.removeEdge(qid, other, edgeType, isDirected = false)
          case Outgoing => graph.literalOps.removeEdge(qid, other, edgeType, isDirected = true)
          case Incoming => graph.literalOps.removeEdge(other, qid, edgeType, isDirected = true)
        }
      }(implicitly, graph.nodeDispatcherEC)
      .map(_ => ())(ExecutionContexts.parasitic)
  }

  private val debugHalfEdgesGetRoute = debugOpsHalfEdgesGet.implementedByAsync {
    case (qid, (atTime, limit, edgeDirOpt, otherOpt, edgeTypeOpt)) =>
      val edgeDirOpt2 = edgeDirOpt.map(fromEdgeDirection)
      graph.literalOps
        .getHalfEdges(qid, edgeTypeOpt.map(Symbol.apply), edgeDirOpt2, otherOpt, limit, atTime)
        .map(_.toVector.map { case HalfEdge(t, d, o) => RestHalfEdge(t.name, toEdgeDirection(d), o) })(
          graph.nodeDispatcherEC
        )
  }

  private val debugPropertyGetRoute = debugOpsPropertyGet.implementedByAsync { case (qid, propKey, atTime) =>
    graph.literalOps
      .getProps(qid, atTime)
      .map(m => m.get(Symbol(propKey)).map(_.deserialized.get).map(qv => QuineValue.toJson(qv)(graph.idProvider)))(
        graph.nodeDispatcherEC
      )
  }

  private val debugPropertyPutRoute = debugOpsPropertyPut.implementedByAsync { case (qid, propKey, value) =>
    graph.literalOps
      .setProp(qid, propKey, QuineValue.fromJson(value))
  }

  private val debugPropertyDeleteRoute = debugOpsPropertyDelete.implementedByAsync { case (qid, propKey) =>
    graph.literalOps.removeProp(qid, propKey)
  }

  final val debugRoutes: Route = {
    debugGetRoute ~
    debugDeleteRoute ~
    debugPostRoute ~
    debugVerboseRoute ~
    debugEdgesGetRoute ~
    debugEdgesPutRoute ~
    debugEdgesDeleteRoute ~
    debugHalfEdgesGetRoute ~
    debugPropertyGetRoute ~
    debugPropertyPutRoute ~
    debugPropertyDeleteRoute
  }
}
