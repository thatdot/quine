package com.thatdot.quine.webapp.queryui

import scala.concurrent.{ExecutionContext, Future, Promise}

import endpoints4s.Invalid
import io.circe.Json

import com.thatdot.api.v2.QueryWebSocketProtocol.QueryInterpreter
import com.thatdot.quine.routes._
import com.thatdot.quine.routes.exts.NamespaceParameter

/** A text query that has been sent to the server and may still be running. */
sealed abstract class RunningQuery[A] {

  /** Completes with the query's final results once the server finishes it, or fails on error. */
  val future: Future[A]
}

object RunningQuery {

  /** A query sent over a REST endpoint. The server offers no way to cancel it. */
  final case class Rest[A](future: Future[A]) extends RunningQuery[A]

  /** A query sent over a WebSocket. `cancel()` asks the server to stop the query, after which
    * `future` completes as cancelled. Compared by reference, so it can key a `Set`.
    */
  final class WebSocket[A](val future: Future[A], val cancel: () => Unit) extends RunningQuery[A]
}

/** The abstraction layer over the server's query endpoints. Its functions send a query to the
  * server and return the results, and make no change to the UI: each caller decides how the
  * results affect it. There is one implementation per [[QueryMethod]] (REST v1/v2, WebSocket
  * v1/v2).
  */
sealed trait QueryRunner {

  /** Given a query whose results are nodes, returns a Future that computes those nodes. The
    * Future holds `None` if the query was cancelled.
    */
  def runNodeQuery(
    query: String,
    language: QueryLanguage,
    parameters: Map[String, Json],
    namespace: NamespaceParameter,
    atTime: Option[Long],
  ): Future[Option[Seq[UiNode[String]]]]

  /** Given a query whose results are edges, returns a Future that computes those edges. The
    * Future holds `None` if the query was cancelled.
    */
  def runEdgeQuery(
    query: String,
    language: QueryLanguage,
    parameters: Map[String, Json],
    namespace: NamespaceParameter,
    atTime: Option[Long],
  ): Future[Option[Seq[UiEdge[String]]]]

  /** Whether this runner can run Gremlin queries. When false, every Gremlin function fails. */
  def supportsGremlin: Boolean

  /** Sends a Cypher query and returns a handle to the running query whose `future` completes
    * once the query finishes, holding `None` if the query was cancelled.
    *
    * `onBatch` is called with all the rows received so far each time more arrive. Over REST,
    * that is once. Over a WebSocket, rows arrive in batches, and `onBatch` is also called once
    * with no rows when the column names arrive.
    */
  def runCypherTextQuery(
    query: String,
    parameters: Map[String, Json],
    namespace: NamespaceParameter,
    atTime: Option[Long],
  )(onBatch: CypherQueryResult => Unit = _ => ()): RunningQuery[Option[Unit]]

  /** Sends a Gremlin query and returns a handle to the running query whose `future` completes
    * once the query finishes, holding `None` if the query was cancelled.
    *
    * `onBatch` is called with all the values received so far each time more arrive. Over REST,
    * that is once.
    */
  def runGremlinTextQuery(
    query: String,
    parameters: Map[String, Json],
    namespace: NamespaceParameter,
    atTime: Option[Long],
  )(onBatch: Seq[Json] => Unit = _ => ()): RunningQuery[Option[Unit]]

  /** Returns one function per query currently running for `namespace`; calling a function asks
    * the server to cancel that query. Returns no functions over REST, where queries can't be
    * cancelled.
    */
  def activeQueryCancellers(namespace: NamespaceParameter): Seq[() => Unit]
}

object QueryRunner {

  /** Sends queries to the API v1 REST endpoints. Node queries ignore their parameters. */
  final class RestV1(routes: ClientRoutes)(implicit ec: ExecutionContext) extends QueryRunner {

    def runNodeQuery(
      query: String,
      language: QueryLanguage,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    ): Future[Option[Seq[UiNode[String]]]] =
      mergeEndpointErrorsIntoFuture(language match {
        case QueryLanguage.Gremlin =>
          routes.gremlinNodesPost((atTime, None, namespace, GremlinQuery(query))).future
        case QueryLanguage.Cypher =>
          routes.cypherNodesPost((atTime, None, namespace, CypherQuery(query))).future
      }).map(Some(_))

    def runEdgeQuery(
      query: String,
      language: QueryLanguage,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    ): Future[Option[Seq[UiEdge[String]]]] =
      mergeEndpointErrorsIntoFuture(language match {
        case QueryLanguage.Gremlin =>
          routes.gremlinEdgesPost((atTime, None, namespace, GremlinQuery(query, parameters))).future
        case QueryLanguage.Cypher =>
          routes.cypherEdgesPost((atTime, None, namespace, CypherQuery(query, parameters))).future
      }).map(Some(_))

    def supportsGremlin: Boolean = true

    def runCypherTextQuery(
      query: String,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    )(onBatch: CypherQueryResult => Unit): RunningQuery.Rest[Option[Unit]] =
      RunningQuery.Rest(
        restTextResults(routes.cypherPost((atTime, None, namespace, CypherQuery(query, parameters))).future, onBatch),
      )

    def runGremlinTextQuery(
      query: String,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    )(onBatch: Seq[Json] => Unit): RunningQuery.Rest[Option[Unit]] =
      RunningQuery.Rest(
        restTextResults(routes.gremlinPost((atTime, None, namespace, GremlinQuery(query, parameters))).future, onBatch),
      )

    def activeQueryCancellers(namespace: NamespaceParameter): Seq[() => Unit] = Nil
  }

  /** Sends queries to the API v2 REST endpoints, which accept only Cypher. Node queries ignore
    * their parameters.
    */
  final class RestV2(routes: ClientRoutes)(implicit ec: ExecutionContext) extends QueryRunner {

    def runNodeQuery(
      query: String,
      language: QueryLanguage,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    ): Future[Option[Seq[UiNode[String]]]] =
      language match {
        case QueryLanguage.Gremlin => Future.failed(gremlinUnsupportedInV2)
        case QueryLanguage.Cypher =>
          mergeEndpointErrorsIntoFuture(
            routes.cypherNodesPostV2((atTime, None, namespace, CypherQuery(query))).future,
          ).map(Some(_))
      }

    def runEdgeQuery(
      query: String,
      language: QueryLanguage,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    ): Future[Option[Seq[UiEdge[String]]]] =
      language match {
        case QueryLanguage.Gremlin => Future.failed(gremlinUnsupportedInV2)
        case QueryLanguage.Cypher =>
          mergeEndpointErrorsIntoFuture(
            routes.cypherEdgesPostV2((atTime, None, namespace, CypherQuery(query, parameters))).future,
          ).map(Some(_))
      }

    def supportsGremlin: Boolean = false

    def runCypherTextQuery(
      query: String,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    )(onBatch: CypherQueryResult => Unit): RunningQuery.Rest[Option[Unit]] =
      RunningQuery.Rest(
        restTextResults(routes.cypherPostV2((atTime, None, namespace, CypherQuery(query, parameters))).future, onBatch),
      )

    def runGremlinTextQuery(
      query: String,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    )(onBatch: Seq[Json] => Unit): RunningQuery.Rest[Option[Unit]] =
      RunningQuery.Rest(Future.failed(gremlinUnsupportedInV2))

    def activeQueryCancellers(namespace: NamespaceParameter): Seq[() => Unit] = Nil
  }

  /** Sends queries over the API v1 WebSocket. `webSocketClient` returns the open client for a
    * namespace.
    */
  final class WebSocketV1(
    webSocketClient: NamespaceParameter => Future[WebSocketQueryClient],
  )(implicit ec: ExecutionContext)
      extends QueryRunner {

    def runNodeQuery(
      query: String,
      language: QueryLanguage,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    ): Future[Option[Seq[UiNode[String]]]] = {
      val nodeCallback = new QueryCallbacks.CollectNodesToFuture()
      val streamingQuery = StreamingQuery(query, parameters, language, atTime, None, Some(100))
      for {
        client <- webSocketClient(namespace)
        _ <- Future.fromTry(client.query(streamingQuery, nodeCallback).toTry)
        results <- nodeCallback.future
      } yield results
    }

    def runEdgeQuery(
      query: String,
      language: QueryLanguage,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    ): Future[Option[Seq[UiEdge[String]]]] = {
      val edgeCallback = new QueryCallbacks.CollectEdgesToFuture()
      val streamingQuery = StreamingQuery(query, parameters, language, atTime, None, Some(100))
      for {
        client <- webSocketClient(namespace)
        _ <- Future.fromTry(client.query(streamingQuery, edgeCallback).toTry)
        results <- edgeCallback.future
      } yield results
    }

    def supportsGremlin: Boolean = true

    def runCypherTextQuery(
      query: String,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    )(onBatch: CypherQueryResult => Unit): RunningQuery.WebSocket[Option[Unit]] = {
      val collector = new BatchCollector(CypherQueryResult(Nil, Nil), onBatch)
      val callbacks = new QueryCallbacks.TabularCallbacks {
        def onTabularResults(columns: Seq[String], batch: Seq[Seq[Json]]): Unit =
          collector.add(soFar => CypherQueryResult(columns, soFar.results ++ batch))
        def onError(message: String): Unit = collector.fail(message)
        def onComplete(): Unit = collector.complete()
        def onQueryStart(
          isReadOnly: Boolean,
          canContainAllNodeScan: Boolean,
          columns: Option[Seq[String]],
        ): Unit = columns.foreach(cols => collector.add(_.copy(columns = cols)))
        def onQueryCancelOk(): Unit = collector.cancelled = true
        def onQueryCancelError(message: String): Unit = ()
      }
      runStreaming(
        namespace,
        StreamingQuery(query, parameters, QueryLanguage.Cypher, atTime, Some(1000), Some(100)),
        callbacks,
        collector,
      )
    }

    def runGremlinTextQuery(
      query: String,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    )(onBatch: Seq[Json] => Unit): RunningQuery.WebSocket[Option[Unit]] = {
      val collector = new BatchCollector(Seq.empty[Json], onBatch)
      val callbacks = new QueryCallbacks.NonTabularCallbacks {
        def onNonTabularResults(batch: Seq[Json]): Unit = collector.add(_ ++ batch)
        def onError(message: String): Unit = collector.fail(message)
        def onComplete(): Unit = collector.complete()
        def onQueryStart(
          isReadOnly: Boolean,
          canContainAllNodeScan: Boolean,
          columns: Option[Seq[String]],
        ): Unit = ()
        def onQueryCancelOk(): Unit = collector.cancelled = true
        def onQueryCancelError(message: String): Unit = ()
      }
      runStreaming(
        namespace,
        StreamingQuery(query, parameters, QueryLanguage.Gremlin, atTime, Some(1000), Some(100)),
        callbacks,
        collector,
      )
    }

    /** Sends a streaming query whose callbacks feed `collector`, and returns a handle to it. */
    private def runStreaming[A](
      namespace: NamespaceParameter,
      streamingQuery: StreamingQuery,
      callbacks: QueryCallbacks,
      collector: BatchCollector[A],
    ): RunningQuery.WebSocket[Option[Unit]] = {
      val clientFut = webSocketClient(namespace)
      val queryIdFut = clientFut.flatMap(client => Future.fromTry(client.query(streamingQuery, callbacks).toTry))
      new RunningQuery.WebSocket(
        queryIdFut.flatMap(_ => collector.result.future),
        () => for { client <- clientFut; queryId <- queryIdFut } { val _ = client.cancelQuery(queryId) },
      )
    }

    def activeQueryCancellers(namespace: NamespaceParameter): Seq[() => Unit] =
      webSocketClient(namespace).value.flatMap(_.toOption).toList.flatMap { client =>
        client.activeQueries.keys.toList.map(queryId => () => { val _ = client.cancelQuery(queryId) })
      }
  }

  /** Sends queries over the API v2 WebSocket, which runs every query as Cypher.
    * `webSocketClientV2` returns the open client for a namespace.
    */
  final class WebSocketV2(webSocketClientV2: NamespaceParameter => Future[V2WebSocketQueryClient])(implicit
    ec: ExecutionContext,
  ) extends QueryRunner {

    private val interpreter: QueryInterpreter = QueryInterpreter.Cypher

    def runNodeQuery(
      query: String,
      language: QueryLanguage,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    ): Future[Option[Seq[UiNode[String]]]] = {
      val cb = new V2QueryCallbacks.CollectNodesToFuture()
      val sq =
        V2StreamingQuery(query, parameters, interpreter = interpreter, atTime = atTime, maxResultBatch = Some(100))
      for {
        client <- webSocketClientV2(namespace)
        _ <- Future.fromTry(client.query(sq, cb).toTry)
        results <- cb.future
      } yield results.map(_.map(n => UiNode(n.id, n.hostIndex, n.label, n.properties)))
    }

    def runEdgeQuery(
      query: String,
      language: QueryLanguage,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    ): Future[Option[Seq[UiEdge[String]]]] = {
      val cb = new V2QueryCallbacks.CollectEdgesToFuture()
      val sq =
        V2StreamingQuery(query, parameters, interpreter = interpreter, atTime = atTime, maxResultBatch = Some(100))
      for {
        client <- webSocketClientV2(namespace)
        _ <- Future.fromTry(client.query(sq, cb).toTry)
        results <- cb.future
      } yield results.map(_.map(e => UiEdge(e.from, e.edgeType, e.to, e.isDirected)))
    }

    def supportsGremlin: Boolean = false

    def runCypherTextQuery(
      query: String,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    )(onBatch: CypherQueryResult => Unit): RunningQuery.WebSocket[Option[Unit]] = {
      val collector = new BatchCollector(CypherQueryResult(Nil, Nil), onBatch)
      val callbacks = new V2QueryCallbacks.TextCallbacks {
        override def onTabularResults(columns: Seq[String], batch: Seq[Seq[Json]]): Unit =
          collector.add(soFar => CypherQueryResult(columns, soFar.results ++ batch))
        def onError(message: String): Unit = collector.fail(message)
        def onComplete(): Unit = collector.complete()
        def onQueryStart(
          isReadOnly: Boolean,
          canContainAllNodeScan: Boolean,
          columns: Option[Seq[String]],
        ): Unit = columns.foreach(cols => collector.add(_.copy(columns = cols)))
        def onQueryCancelOk(): Unit = collector.cancelled = true
        def onQueryCancelError(message: String): Unit = ()
      }
      val sq = V2StreamingQuery(
        query,
        parameters,
        interpreter = interpreter,
        atTime = atTime,
        maxResultBatch = Some(1000),
        resultsWithinMillis = Some(100),
      )
      val clientFut = webSocketClientV2(namespace)
      val queryIdFut = clientFut.flatMap(client => Future.fromTry(client.query(sq, callbacks).toTry))
      new RunningQuery.WebSocket(
        queryIdFut.flatMap(_ => collector.result.future),
        () => for { client <- clientFut; queryId <- queryIdFut } { val _ = client.cancelQuery(queryId) },
      )
    }

    def runGremlinTextQuery(
      query: String,
      parameters: Map[String, Json],
      namespace: NamespaceParameter,
      atTime: Option[Long],
    )(onBatch: Seq[Json] => Unit): RunningQuery.WebSocket[Option[Unit]] =
      new RunningQuery.WebSocket(Future.failed(gremlinUnsupportedInV2), () => ())

    def activeQueryCancellers(namespace: NamespaceParameter): Seq[() => Unit] =
      webSocketClientV2(namespace).value.flatMap(_.toOption).toList.flatMap { client =>
        client.activeQueries.keys.toList.map(queryId => () => { val _ = client.cancelQuery(queryId) })
      }
  }

  // ── Helpers shared by the implementations ──

  private def gremlinUnsupportedInV2: Exception = new Exception("Gremlin is not supported in APIv2")

  private def invalidToException(invalid: Invalid): Exception = new Exception(invalid.errors mkString "\n")

  private def mergeEndpointErrorsIntoFuture[A](fut: Future[Either[Invalid, Option[A]]])(implicit
    ec: ExecutionContext,
  ): Future[A] =
    fut.flatMap { either =>
      Future.fromTry {
        either.left
          .map(invalidToException)
          .flatMap(_.toRight(new NoSuchElementException()))
          .toTry
      }
    }

  /** Given a REST query's response, returns a Future that passes its results to `onBatch` once
    * they arrive.
    */
  private def restTextResults[A](
    response: Future[Either[Invalid, Option[A]]],
    onBatch: A => Unit,
  )(implicit ec: ExecutionContext): Future[Option[Unit]] =
    mergeEndpointErrorsIntoFuture(response).map(results => Some(onBatch(results)))

  /** Accumulates a streaming text query's batches into a value of type `A` (starting from
    * `initial`), reporting the accumulated value to `onBatch` after every change. `result`
    * completes (with `None` if the query was cancelled) when the WebSocket callbacks report
    * completion or an error.
    */
  final private class BatchCollector[A](initial: A, onBatch: A => Unit) {
    val result: Promise[Option[Unit]] = Promise()
    var cancelled: Boolean = false
    private var soFar: A = initial

    def add(update: A => A): Unit = {
      soFar = update(soFar)
      onBatch(soFar)
    }

    def fail(message: String): Unit = {
      val _ = result.tryFailure(new Exception(message))
    }

    def complete(): Unit = {
      val _ = result.trySuccess(if (cancelled) None else Some(()))
    }
  }
}
