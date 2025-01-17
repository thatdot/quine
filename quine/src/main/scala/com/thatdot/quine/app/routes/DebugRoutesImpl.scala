package com.thatdot.quine.app.routes

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.util.Timeout

import com.thatdot.common.logging.Log._
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph._
import com.thatdot.quine.graph.messaging.LiteralMessage.{
  DgnWatchableEventIndexSummary,
  LocallyRegisteredStandingQuery,
  NodeInternalState,
  SqStateResult,
  SqStateResults,
}
import com.thatdot.quine.model
import com.thatdot.quine.model.{EdgeDirection => _, _}
import com.thatdot.quine.routes.EdgeDirection._
import com.thatdot.quine.routes._
import com.thatdot.quine.routes.exts.NamespaceParameter

/** The Pekko HTTP implementation of [[DebugOpsRoutes]] */
trait DebugRoutesImpl
    extends DebugOpsRoutes
    with com.thatdot.quine.app.routes.exts.ServerQuineEndpoints
    with com.thatdot.quine.app.routes.exts.circe.JsonEntitiesFromSchemas {

  implicit protected def logConfig: LogConfig

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
    implicit val dgnLocalEventIndexSummarySchema: Record[DgnWatchableEventIndexSummary] =
      genericRecord[DgnWatchableEventIndexSummary]
    implicit val neSchema: Tagged[NodeEvent] = genericTagged[NodeEvent]
    implicit val newtSchema: Record[NodeEvent.WithTime[NodeEvent]] = genericRecord[NodeEvent.WithTime[NodeEvent]]
    implicit val sqResult: Record[SqStateResult] = genericRecord[SqStateResult]
    implicit val sqResults: Record[SqStateResults] = genericRecord[SqStateResults]
    genericRecord[NodeInternalState]
  }

  implicit def graph: LiteralOpsGraph
  implicit def timeout: Timeout

  private val debugGetRoute = debugOpsGet.implementedByAsync {
    case (qid: QuineId, atTime: AtTime, namespaceParam: NamespaceParameter) =>
      graph.requiredGraphIsReadyFuture {
        val propsF = graph.literalOps(namespaceFromParam(namespaceParam)).getProps(qid, atTime = atTime)
        val edgesF = graph.literalOps(namespaceFromParam(namespaceParam)).getEdges(qid, atTime = atTime)
        propsF
          .zip(edgesF)
          .map { case (props, edges) =>
            LiteralNode(
              props.map { case (k, v) => k.name -> QuineValue.toJson(v.deserialized.get)(graph.idProvider, logConfig) },
              edges.toSeq.map { case HalfEdge(t, d, o) => RestHalfEdge(t.name, toEdgeDirection(d), o) },
            )
          }(graph.nodeDispatcherEC)
      }
  }

  private val debugPostRoute = debugOpsPut.implementedByAsync {
    case (qid: QuineId, namespaceParam: NamespaceParameter, node: LiteralNode[QuineId]) =>
      graph.requiredGraphIsReadyFuture {
        val namespaceId = namespaceFromParam(namespaceParam)
        val propsF = Future.traverse(node.properties.toList) { case (typ, value) =>
          graph.literalOps(namespaceId).setProp(qid, typ, QuineValue.fromJson(value))
        }(implicitly, graph.nodeDispatcherEC)
        val edgesF = Future.traverse(node.edges) {
          case RestHalfEdge(typ, Outgoing, to) =>
            graph.literalOps(namespaceId).addEdge(qid, to, typ, isDirected = true)
          case RestHalfEdge(typ, Incoming, to) =>
            graph.literalOps(namespaceId).addEdge(to, qid, typ, isDirected = true)
          case RestHalfEdge(typ, Undirected, to) =>
            graph.literalOps(namespaceId).addEdge(qid, to, typ, isDirected = false)
        }(implicitly, graph.nodeDispatcherEC)
        propsF.flatMap(_ => edgesF)(ExecutionContext.parasitic).map(_ => ())(ExecutionContext.parasitic)
      }
  }

  private val debugDeleteRoute = debugOpsDelete.implementedByAsync {
    case (qid: QuineId, namespaceParam: NamespaceParameter) =>
      graph.requiredGraphIsReadyFuture {
        graph.literalOps(namespaceFromParam(namespaceParam)).deleteNode(qid)
      }
  }

  protected val debugVerboseRoute: Route = debugOpsVerbose.implementedByAsync {
    case (qid: QuineId, atTime: AtTime, namespaceParam: NamespaceParameter) =>
      graph.requiredGraphIsReadyFuture {
        graph
          .literalOps(namespaceFromParam(namespaceParam))
          .logState(qid, atTime)
          .map(nodeInternalStateSchema.encoder(_))(graph.nodeDispatcherEC)
      }
  }

  private val debugEdgesGetRoute = debugOpsEdgesGet.implementedByAsync {
    case (qid, (atTime, limit, edgeDirOpt, otherOpt, edgeTypeOpt, namespaceParam)) =>
      graph.requiredGraphIsReadyFuture {
        val edgeDirOpt2 = edgeDirOpt.map(fromEdgeDirection)
        graph
          .literalOps(namespaceFromParam(namespaceParam))
          .getEdges(qid, edgeTypeOpt.map(Symbol.apply), edgeDirOpt2, otherOpt, limit, atTime)
          .map(_.toVector.map { case HalfEdge(t, d, o) => RestHalfEdge(t.name, toEdgeDirection(d), o) })(
            graph.nodeDispatcherEC,
          )
      }
  }

  private val debugEdgesPutRoute = debugOpsEdgesPut.implementedByAsync { case (qid, namespaceParam, edges) =>
    graph.requiredGraphIsReadyFuture {
      Future
        .traverse(edges) { case RestHalfEdge(edgeType, edgeDir, other) =>
          edgeDir match {
            case Undirected =>
              graph.literalOps(namespaceFromParam(namespaceParam)).addEdge(qid, other, edgeType, isDirected = false)
            case Outgoing =>
              graph.literalOps(namespaceFromParam(namespaceParam)).addEdge(qid, other, edgeType, isDirected = true)
            case Incoming =>
              graph.literalOps(namespaceFromParam(namespaceParam)).addEdge(other, qid, edgeType, isDirected = true)
          }
        }(implicitly, graph.nodeDispatcherEC)
        .map(_ => ())(ExecutionContext.parasitic)
    }
  }

  private val debugEdgesDeleteRoute = debugOpsEdgeDelete.implementedByAsync { case (qid, namespaceParam, edges) =>
    graph.requiredGraphIsReadyFuture {
      Future
        .traverse(edges) { case RestHalfEdge(edgeType, edgeDir, other) =>
          edgeDir match {
            case Undirected =>
              graph.literalOps(namespaceFromParam(namespaceParam)).removeEdge(qid, other, edgeType, isDirected = false)
            case Outgoing =>
              graph.literalOps(namespaceFromParam(namespaceParam)).removeEdge(qid, other, edgeType, isDirected = true)
            case Incoming =>
              graph.literalOps(namespaceFromParam(namespaceParam)).removeEdge(other, qid, edgeType, isDirected = true)
          }
        }(implicitly, graph.nodeDispatcherEC)
        .map(_ => ())(ExecutionContext.parasitic)
    }
  }

  private val debugHalfEdgesGetRoute = debugOpsHalfEdgesGet.implementedByAsync {
    case (qid, (atTime, limit, edgeDirOpt, otherOpt, edgeTypeOpt, namespaceParam)) =>
      graph.requiredGraphIsReadyFuture {
        val edgeDirOpt2 = edgeDirOpt.map(fromEdgeDirection)
        graph
          .literalOps(namespaceFromParam(namespaceParam))
          .getHalfEdges(qid, edgeTypeOpt.map(Symbol.apply), edgeDirOpt2, otherOpt, limit, atTime)
          .map(_.toVector.map { case HalfEdge(t, d, o) => RestHalfEdge(t.name, toEdgeDirection(d), o) })(
            graph.nodeDispatcherEC,
          )
      }
  }

  private val debugPropertyGetRoute = debugOpsPropertyGet.implementedByAsync {
    case (qid, propKey, atTime, namespaceParam) =>
      graph.requiredGraphIsReadyFuture {
        graph
          .literalOps(namespaceFromParam(namespaceParam))
          .getProps(qid, atTime)
          .map(m =>
            m.get(Symbol(propKey)).map(_.deserialized.get).map(qv => QuineValue.toJson(qv)(graph.idProvider, logConfig)),
          )(
            graph.nodeDispatcherEC,
          )
      }
  }

  private val debugPropertyPutRoute = debugOpsPropertyPut.implementedByAsync {
    case (qid, propKey, namespaceParam, value) =>
      graph.requiredGraphIsReadyFuture {
        graph
          .literalOps(namespaceFromParam(namespaceParam))
          .setProp(qid, propKey, QuineValue.fromJson(value))
      }
  }

  private val debugPropertyDeleteRoute = debugOpsPropertyDelete.implementedByAsync {
    case (qid, propKey, namespaceParam) =>
      graph.requiredGraphIsReadyFuture {
        graph.literalOps(namespaceFromParam(namespaceParam)).removeProp(qid, propKey)
      }
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
