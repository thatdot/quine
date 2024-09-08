package com.thatdot.quine.app.routes

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.Timeout

import io.circe.Json

import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.graph.{CypherOpsGraph, LiteralOpsGraph, NamespaceId}
import com.thatdot.quine.gremlin._
import com.thatdot.quine.model._
import com.thatdot.quine.routes.{CypherQueryResult, GremlinQuery, QueryUiRoutes, UiEdge, UiNode}
import com.thatdot.quine.util.Log._

trait QueryUiRoutesImpl
    extends QueryUiRoutes
    with QueryUiCypherApiMethods
    with endpoints4s.pekkohttp.server.Endpoints
    with exts.circe.JsonEntitiesFromSchemas
    with exts.ServerQuineEndpoints
    with exts.ServerRequestTimeoutOps
    with LazySafeLogging {

  val gremlin: GremlinQueryRunner

  implicit def graph: LiteralOpsGraph with CypherOpsGraph
  implicit def idProvider: QuineIdProvider
  implicit def timeout: Timeout
  implicit def materializer: Materializer

  private[this] lazy val idProv = idProvider
  private[this] lazy val CustomIdTypeClassTag: ClassTag[idProv.CustomIdType] = idProv.customIdTag

  /** Compute the host of a quine ID */
  def hostIndex(qid: QuineId): Int = 0

  // This is how Gremlin values will be formatted as JSON
  // NB: this is tuned to consume values coming out of the Gremlin interpreter
  private def writeGremlinValue(any: Any): Json = any match {
    // Null value
    case null | () => Json.Null

    // Option
    case None => Json.Null
    case Some(x) => writeGremlinValue(x)

    // Numbers
    case n: Byte => Json.fromInt(n.intValue)
    case n: Int => Json.fromInt(n)
    case n: Long => Json.fromLong(n)
    case n: Float => Json.fromFloatOrString(n)
    case n: Double => Json.fromDoubleOrString(n)
    case n: java.lang.Long => Json.fromLong(n)
    case n: java.lang.Double => Json.fromDoubleOrString(n)

    // Strings
    case s: String => Json.fromString(s)

    // Booleans
    case b: Boolean => Json.fromBoolean(b)
    case b: java.lang.Boolean => Json.fromBoolean(b)

    // Lists
    case l: java.util.List[_] => writeGremlinValue(l.asScala)
    case l: List[_] => Json.fromValues(l.map(writeGremlinValue))
    case a: Array[_] => Json.fromValues(a.map(writeGremlinValue))
    case a: Vector[_] => Json.fromValues(a.map(writeGremlinValue))

    // Maps
    case m: java.util.Map[_, _] => writeGremlinValue(m.asScala)
    case m: Map[_, _] => Json.fromFields(m map { case (k, v) => (k.toString, writeGremlinValue(v)) })

    // Vertex and edges
    case Vertex(qid) => Json.fromString(s"Vertex($qid)")
    case Edge(src, lbl, tgt) => Json.fromString(s"Edge($src, ${lbl.name}, $tgt)")

    // Custom id type
    case CustomIdTypeClassTag(a) => Json.fromString(idProv.customIdToString(a))

    // Other: Any custom 'toString'
    case o => Json.fromString(o.toString)
  }

  private def guessGremlinParameters(params: Map[String, Json]): Map[Symbol, QuineValue] =
    params.map { case (k, v) => Symbol(k) -> QuineValue.fromJson(v) }

  /** Given a [[QuineId]], query out a [[UiNode]]
    *
    * @note this is not used by Cypher because those nodes already have the needed information!
    * @param id ID of the node
    * @param namespace the namespace in which to run this query
    * @param atTime possibly historical time to query
    * @return representation of the node for the UI
    */
  private def queryUiNode(
    id: QuineId,
    namespace: NamespaceId,
    atTime: AtTime,
  ): Future[UiNode[QuineId]] =
    graph
      .literalOps(namespace)
      .getPropsAndLabels(id, atTime)
      .map { case (props, labels) =>
        val parsedProperties = props.map { case (propKey, pickledValue) =>
          val unpickledValue = pickledValue.deserialized.fold[Any](
            _ => pickledValue.serialized,
            _.underlyingJvmValue,
          )
          propKey.name -> writeGremlinValue(unpickledValue)
        }

        val nodeLabel = if (labels.exists(_.nonEmpty)) {
          labels.get.map(_.name).mkString(":")
        } else {
          "ID: " + id.pretty
        }

        UiNode(
          id = id,
          hostIndex = hostIndex(id),
          label = nodeLabel,
          properties = parsedProperties,
        )
      }(graph.shardDispatcherEC)

  /** Post-process UI nodes. This serves as a hook for last minute modifications to the nodes sen
    * out to the UI.
    *
    * @param uiNode UI node to modify
    * @return updated UI node
    */
  protected def transformUiNode(uiNode: UiNode[QuineId]): UiNode[QuineId] = uiNode

  /** Query nodes with a given gremlin query
    *
    * @note this filters out nodes whose IDs are not supported by the provider
    * @param query Gremlin query expected to return nodes
    * @param namespace the namespace in which to run this query
    * @param atTime possibly historical time to query
    * @return nodes produced by the query
    */
  final def queryGremlinNodes(
    query: GremlinQuery,
    namespace: NamespaceId,
    atTime: AtTime,
  ): Source[UiNode[QuineId], NotUsed] =
    gremlin
      .queryExpecting[Vertex](
        query.text,
        guessGremlinParameters(query.parameters),
        namespace,
        atTime,
      )
      .mapAsync(parallelism = 4)((vertex: Vertex) => queryUiNode(vertex.id, namespace, atTime))
      .map(transformUiNode)

  /** Query edges with a given gremlin query
    *
    * @note this filters out nodes whose IDs are not supported by the provider
    * @param query Gremlin query expected to return edges
    * @param namespace the namespace in which to run this query
    * @param atTime possibly historical time to query
    * @return edges produced by the query
    */
  final def queryGremlinEdges(
    query: GremlinQuery,
    namespace: NamespaceId,
    atTime: AtTime,
  ): Source[UiEdge[QuineId], NotUsed] =
    gremlin
      .queryExpecting[Edge](
        query.text,
        guessGremlinParameters(query.parameters),
        namespace,
        atTime,
      )
      .map { case Edge(src, lbl, tgt) => UiEdge(from = src, to = tgt, edgeType = lbl.name) }

  /** Query anything with a given Gremlin query
    *
    * @param query Gremlin query
    * @param namespace the namespace in which to run this query
    * @param atTime possibly historical time to query
    * @return data produced by the query formatted as JSON
    */
  final def queryGremlinGeneric(query: GremlinQuery, namespace: NamespaceId, atTime: AtTime): Source[Json, NotUsed] =
    gremlin
      .query(query.text, guessGremlinParameters(query.parameters), namespace, atTime)
      .map[Json](writeGremlinValue)

  // This could be made more general, but the dependency on ClientErrors makes it get "stuck in the cake" here and some
  // other route implementation traits that share similar private methods.
  final private def ifNamespaceFound[A](namespaceId: NamespaceId)(
    ifFound: => Future[Either[ClientErrors, A]],
  ): Future[Either[ClientErrors, Option[A]]] =
    if (!graph.getNamespaces.contains(namespaceId)) Future.successful(Right(None))
    else ifFound.map(_.map(Some(_)))(ExecutionContext.parasitic)

  // The Query UI relies heavily on a couple Gremlin endpoints for making queries.
  final val gremlinApiRoute: Route = {
    def catchGremlinException[A](futA: => Future[A]): Future[Either[ClientErrors, A]] =
      Future
        .fromTry(Try(futA))
        .flatten
        .transform {
          case Success(a) => Success(Right(a))
          case Failure(qge: QuineGremlinException) => Success(Left(endpoints4s.Invalid(qge.toString)))
          case Failure(err) => Failure(err)
        }(graph.shardDispatcherEC)

    gremlinPost.implementedByAsyncWithRequestTimeout(_._2) { case ((atTime, _, namespaceParam, query), t) =>
      graph.requiredGraphIsReadyFuture {
        val ns = namespaceFromParam(namespaceParam)
        ifNamespaceFound(ns)(catchGremlinException {
          queryGremlinGeneric(query, ns, atTime)
            .via(Util.completionTimeoutOpt(t))
            .named(s"gremlin-query-atTime-${atTime.fold("none")(_.millis.toString)}")
            .runWith(Sink.seq)
        })
      }
    } ~
    gremlinNodesPost.implementedByAsyncWithRequestTimeout(_._2) { case ((atTime, _, namespaceParam, query), t) =>
      graph.requiredGraphIsReadyFuture {
        val ns = namespaceFromParam(namespaceParam)
        ifNamespaceFound(ns)(catchGremlinException {
          queryGremlinNodes(query, ns, atTime)
            .via(Util.completionTimeoutOpt(t))
            .named(s"gremlin-node-query-atTime-${atTime.fold("none")(_.millis.toString)}")
            .runWith(Sink.seq)
        })
      }
    } ~
    gremlinEdgesPost.implementedByAsyncWithRequestTimeout(_._2) { case ((atTime, _, namespaceParam, query), t) =>
      graph.requiredGraphIsReadyFuture {
        val ns = namespaceFromParam(namespaceParam)
        ifNamespaceFound(ns)(catchGremlinException {
          queryGremlinEdges(query, ns, atTime)
            .via(Util.completionTimeoutOpt(t))
            .named(s"gremlin-edge-query-atTime-${atTime.fold("none")(_.millis.toString)}")
            .runWith(Sink.seq)
        })
      }
    }
  }

  // The Query UI relies heavily on a couple Cypher endpoints for making queries.
  final val cypherApiRoute: Route = {
    def catchCypherException[A](futA: => Future[A]): Future[Either[ClientErrors, A]] =
      Future
        .fromTry(Try(futA))
        .flatten
        .transform {
          case Success(a) => Success(Right(a))
          case Failure(qce: CypherException) => Success(Left(endpoints4s.Invalid(qce.pretty)))
          case Failure(err) => Failure(err)
        }(ExecutionContext.parasitic)

    cypherPost.implementedByAsyncWithRequestTimeout(_._2) { case ((atTime, _, namespaceParam, query), t) =>
      graph.requiredGraphIsReadyFuture {
        val ns = namespaceFromParam(namespaceParam)
        ifNamespaceFound(ns)(catchCypherException {
          val (columns, results, isReadOnly, _) =
            queryCypherGeneric(query, ns, atTime) // TODO read canContainAllNodeScan
          results
            .via(Util.completionTimeoutOpt(t, allowTimeout = isReadOnly))
            .named(s"cypher-query-atTime-${atTime.fold("none")(_.millis.toString)}")
            .runWith(Sink.seq)
            .map(CypherQueryResult(columns, _))(ExecutionContext.parasitic)
        })
      }
    } ~
    cypherNodesPost.implementedByAsyncWithRequestTimeout(_._2) { case ((atTime, _, namespaceParam, query), t) =>
      graph.requiredGraphIsReadyFuture {
        val ns = namespaceFromParam(namespaceParam)
        ifNamespaceFound(ns)(catchCypherException {
          val (results, isReadOnly, _) =
            queryCypherNodes(query, ns, atTime) // TODO read canContainAllNodeScan
          results
            .via(Util.completionTimeoutOpt(t, allowTimeout = isReadOnly))
            .named(s"cypher-nodes-query-atTime-${atTime.fold("none")(_.millis.toString)}")
            .runWith(Sink.seq)
        })
      }
    } ~
    cypherEdgesPost.implementedByAsyncWithRequestTimeout(_._2) { case ((atTime, _, namespaceParam, query), t) =>
      graph.requiredGraphIsReadyFuture {
        val ns = namespaceFromParam(namespaceParam)
        ifNamespaceFound(ns)(catchCypherException {
          val (results, isReadOnly, _) =
            queryCypherEdges(query, ns, atTime) // TODO read canContainAllNodeScan
          results
            .via(Util.completionTimeoutOpt(t, allowTimeout = isReadOnly))
            .named(s"cypher-edges-query-atTime-${atTime.fold("none")(_.millis.toString)}")
            .runWith(Sink.seq)
        })
      }
    }
  }

  final val queryUiRoutes: Route = {
    gremlinApiRoute ~
    cypherApiRoute
  }
}
