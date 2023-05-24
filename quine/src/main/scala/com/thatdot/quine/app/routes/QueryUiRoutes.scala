package com.thatdot.quine.app.routes

import scala.compat.ExecutionContexts
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import akka.NotUsed
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout

import com.typesafe.scalalogging.LazyLogging
import io.circe.Json

import com.thatdot.quine.compiler.cypher
import com.thatdot.quine.graph.cypher.{
  CypherException,
  Expr => CypherExpr,
  QueryResults,
  Type => CypherType,
  Value => CypherValue
}
import com.thatdot.quine.graph.{CypherOpsGraph, LiteralOpsGraph}
import com.thatdot.quine.gremlin._
import com.thatdot.quine.model._
import com.thatdot.quine.routes.{CypherQuery, CypherQueryResult, GremlinQuery, QueryUiRoutes, UiEdge, UiNode}

trait QueryUiRoutesImpl
    extends QueryUiRoutes
    with endpoints4s.akkahttp.server.Endpoints
    with exts.circe.JsonEntitiesFromSchemas
    with exts.ServerQuineEndpoints
    with exts.ServerRequestTimeoutOps
    with LazyLogging {

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
    params.map { case (k, v) => (Symbol(k) -> QuineValue.fromJson(v)) }

  private def guessCypherParameters(params: Map[String, Json]): Map[String, CypherValue] =
    params.map { case (k, v) => (k -> CypherExpr.fromQuineValue(QuineValue.fromJson(v))) }

  /** Given a [[QuineId]], query out a [[UiNode]]
    *
    * @note this is not used by Cypher because those nodes already have the needed information!
    * @param id ID of the node
    * @param atTime possibly historical time to query
    * @return representation of the node for the UI
    */
  private def queryUiNode(
    id: QuineId,
    atTime: AtTime
  ): Future[UiNode[QuineId]] =
    graph.literalOps
      .getPropsAndLabels(id, atTime)
      .map { case (props, labels) =>
        val parsedProperties = props.map { case (propKey, pickledValue) =>
          val unpickledValue = pickledValue.deserialized.fold[Any](
            _ => pickledValue.serialized,
            _.underlyingJvmValue
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
          properties = parsedProperties
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
    * @param atTime possibly historical time to query
    * @return nodes produced by the query
    */
  final def queryGremlinNodes(query: GremlinQuery, atTime: AtTime): Source[UiNode[QuineId], NotUsed] =
    gremlin
      .queryExpecting[Vertex](query.text, guessGremlinParameters(query.parameters), atTime)
      .mapAsync(parallelism = 4)((vertex: Vertex) => queryUiNode(vertex.id, atTime))
      .map(transformUiNode)

  /** Query edges with a given gremlin query
    *
    * @note this filters out nodes whose IDs are not supported by the provider
    * @param query Gremlin query expected to return edges
    * @param atTime possibly historical time to query
    * @return edges produced by the query
    */
  final def queryGremlinEdges(query: GremlinQuery, atTime: AtTime): Source[UiEdge[QuineId], NotUsed] =
    gremlin
      .queryExpecting[Edge](query.text, guessGremlinParameters(query.parameters), atTime)
      .map { case Edge(src, lbl, tgt) => UiEdge(from = src, to = tgt, edgeType = lbl.name) }

  /** Query anything with a given Gremlin query
    *
    * @param query Gremlin query
    * @param atTime possibly historical time to query
    * @return data produced by the query formatted as JSON
    */
  final def queryGremlinGeneric(query: GremlinQuery, atTime: AtTime): Source[Json, NotUsed] =
    gremlin
      .query(query.text, guessGremlinParameters(query.parameters), atTime)
      .map[Json](writeGremlinValue)

  /** Query nodes with a given Cypher query
    *
    * @note this filters out nodes whose IDs are not supported by the provider
    *
    * @param query Cypher query expected to return nodes
    * @param atTime possibly historical time to query
    * @return tuple of nodes produced by the query, whether the query is read-only, and whether the query may cause full node scan
    */
  final def queryCypherNodes(
    query: CypherQuery,
    atTime: AtTime
  ): (Source[UiNode[QuineId], NotUsed], Boolean, Boolean) = {
    val res: QueryResults = cypher.queryCypherValues(
      query.text,
      parameters = guessCypherParameters(query.parameters),
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
    * @param atTime possibly historical time to query
    * @param requestTimeout timeout signalling output results no longer matter
    * @return tuple of edges produced by the query, readonly, and canContainAllNodeScan
    */
  final def queryCypherEdges(
    query: CypherQuery,
    atTime: AtTime,
    requestTimeout: Duration = Duration.Inf
  ): (Source[UiEdge[QuineId], NotUsed], Boolean, Boolean) = {
    val res: QueryResults = cypher.queryCypherValues(
      query.text,
      parameters = guessCypherParameters(query.parameters),
      atTime = atTime
    )

    val results = res.results
      .mapConcat(identity)
      .map[UiEdge[QuineId]] {
        case CypherExpr.Relationship(src, lbl, props @ _, tgt) =>
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
    * @param atTime possibly historical time to query
    * @return data produced by the query formatted as JSON
    */
  final def queryCypherGeneric(
    query: CypherQuery,
    atTime: AtTime
  ): (Seq[String], Source[Seq[Json], NotUsed], Boolean, Boolean) = {

    // TODO: remove `PROFILE` here too
    val ExplainedQuery = raw"(?is)\s*explain\s+(.*)".r
    val (explainQuery, queryText) = query.text match {
      case ExplainedQuery(toExplain) => true -> toExplain
      case other => false -> other
    }

    val res: QueryResults = cypher.queryCypherValues(
      queryText,
      parameters = guessCypherParameters(query.parameters),
      atTime = atTime
    )

    if (!explainQuery) {
      val columns = res.columns.map(_.name)
      val bodyRows = res.results.map(row => row.map(CypherValue.toJson))
      (columns, bodyRows, res.compiled.isReadOnly, res.compiled.canContainAllNodeScan)
    } else {
      logger.debug(s"User requested EXPLAIN of query: ${res.compiled.query}")
      val plan = cypher.Plan.fromQuery(res.compiled.query).toValue
      (Vector("plan"), Source.single(Seq(CypherValue.toJson(plan))), true, false)
    }
  }

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

    gremlinPost.implementedByAsyncWithRequestTimeout(_._2) { case ((atTime, _, query), t) =>
      catchGremlinException {
        queryGremlinGeneric(query, atTime)
          .via(Util.completionTimeoutOpt(t))
          .named(s"gremlin-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .runWith(Sink.seq)
      }
    } ~
    gremlinNodesPost.implementedByAsyncWithRequestTimeout(_._2) { case ((atTime, _, query), t) =>
      catchGremlinException {
        queryGremlinNodes(query, atTime)
          .via(Util.completionTimeoutOpt(t))
          .named(s"gremlin-node-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .runWith(Sink.seq)
      }
    } ~
    gremlinEdgesPost.implementedByAsyncWithRequestTimeout(_._2) { case ((atTime, _, query), t) =>
      catchGremlinException {
        queryGremlinEdges(query, atTime)
          .via(Util.completionTimeoutOpt(t))
          .named(s"gremlin-edge-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .runWith(Sink.seq)
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
        }(ExecutionContexts.parasitic)

    cypherPost.implementedByAsyncWithRequestTimeout(_._2) { case ((atTime, _, query), t) =>
      catchCypherException {
        val (columns, results, isReadOnly, _) = queryCypherGeneric(query, atTime) // TODO read canContainAllNodeScan
        results
          .via(Util.completionTimeoutOpt(t, allowTimeout = isReadOnly))
          .named(s"cypher-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .runWith(Sink.seq)
          .map(CypherQueryResult(columns, _))(ExecutionContexts.parasitic)
      }
    } ~
    cypherNodesPost.implementedByAsyncWithRequestTimeout(_._2) { case ((atTime, _, query), t) =>
      catchCypherException {
        val (results, isReadOnly, _) = queryCypherNodes(query, atTime) // TODO read canContainAllNodeScan
        results
          .via(Util.completionTimeoutOpt(t, allowTimeout = isReadOnly))
          .named(s"cypher-nodes-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .runWith(Sink.seq)
      }
    } ~
    cypherEdgesPost.implementedByAsyncWithRequestTimeout(_._2) { case ((atTime, _, query), t) =>
      catchCypherException {
        val (results, isReadOnly, _) = queryCypherEdges(query, atTime) // TODO read canContainAllNodeScan
        results
          .via(Util.completionTimeoutOpt(t, allowTimeout = isReadOnly))
          .named(s"cypher-edges-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .runWith(Sink.seq)
      }
    }
  }

  final val queryUiRoutes: Route = {
    gremlinApiRoute ~
    cypherApiRoute
  }
}
