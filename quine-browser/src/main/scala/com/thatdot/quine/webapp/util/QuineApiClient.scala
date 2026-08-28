package com.thatdot.quine.webapp.util

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}
import org.scalajs.dom
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes.exts.NamespaceParameter
import com.thatdot.quine.routes.{
  ClientRoutes,
  MetricsReport,
  QueryLanguage,
  SampleQuery,
  ShardInMemoryLimit,
  UiNodeAppearance,
  UiNodeQuickQuery,
}
import com.thatdot.quine.webapp.AuthEvents
import com.thatdot.quine.webapp.v2api.V2ApiTypes._
import com.thatdot.quine.webapp.v2api.{QuickQueryConversions, V2Paths}

/** Lightweight V2 API fetch-and-poll utilities shared across components.
  *
  * Provides a namespace-aware `standingQueries` feed and the generic `fetchV2`/`poll`
  * primitives that back it. Components consume a [[QuineApiClient.Feed]] without
  * knowing where or how often data is fetched.
  */
object QuineApiClient {

  val PollIntervalMs: Int = 5000

  /** Faster cadence for the metrics feeds, whose dashboard consumer expects 2s updates. */
  val MetricsPollIntervalMs: Int = 2000

  /** Slower cadence for the streams-page list feeds (standing queries, ingests): a
    * refresh rebuilds row content under the user's cursor, so ticks are kept sparse.
    * Mutations refetch immediately via the DataService `Refresh*` commands regardless.
    */
  val StreamsPollIntervalMs: Int = 10000

  /** Faster cadence for the background-query execution feed: the query bar's popover shows
    * in-flight runs, and a user who just started one expects it to appear promptly.
    */
  val BackgroundQueryPollIntervalMs: Int = 3000

  /** A polled feed: successful values and failure messages on separate streams. */
  final case class Feed[A](values: EventStream[A], errors: EventStream[String]) {

    /** Fold the feed into a [[Pot]] lifecycle signal: `Pending` until the first result,
      * `Ready` on success, and on failure `FailedStale` (keeping the last good value) when
      * data was already loaded, else `Failed`.
      */
    def potSignal: Signal[Pot[A]] =
      EventStream
        .merge(values.map(Right(_)), errors.map(Left(_)))
        .scanLeft(Pot.Pending: Pot[A]) {
          case (_, Right(value)) => Pot.Ready(value)
          case (prev, Left(msg)) =>
            prev.toOption match {
              case Some(stale) => Pot.FailedStale(stale, msg)
              case None => Pot.Failed(msg)
            }
        }
        .distinct
  }

  /** Poll `fetch` on a fixed interval, splitting results into success/error streams. */
  def poll[A](fetch: => Future[A], intervalMs: Int = PollIntervalMs): Feed[A] = {
    val ticks = PollingStream(intervalMs) {
      fetch.transform(scala.util.Success(_))
    }
    Feed(
      values = ticks.collect { case scala.util.Success(value) => value },
      errors = ticks.collect { case scala.util.Failure(t) => errorMessage(t) },
    )
  }

  /** Common transport for V2 API calls: resolves base URL, maps 401 onto the auth bus,
    * and fails any non-2xx. The successful response is handed back unread.
    */
  private def requestV2(path: String, routes: ClientRoutes, init: dom.RequestInit): Future[dom.Response] = {
    val url = routes.baseUrlOpt match {
      case Some(base) => s"${base.stripSuffix("/")}/$path"
      case None => path
    }
    dom
      .fetch(url, init)
      .toFuture
      .recoverWith { case _ =>
        Future.failed(new RuntimeException("Cannot reach server"))
      }
      .flatMap { response =>
        if (response.status == 401) {
          AuthEvents.unauthorized.emit(())
          failWithServerMessage(response)
        } else if (!response.ok) {
          failWithServerMessage(response)
        } else Future.successful(response)
      }
  }

  /** Fail a non-2xx response with the server's own error message when it sends one.
    *
    * V2 error bodies follow the AIP-193 envelope `{"error": {"code", "status", "message", ...}}`.
    * Surfacing `error.message` lets callers (e.g. the graph-projection save toast) show *why* a
    * request was rejected — a read-only violation, a name conflict — instead of a bare
    * "Server returned 400". Falls back to the status line when the body is absent, unparseable,
    * or not in that shape.
    */
  private def failWithServerMessage(response: dom.Response): Future[dom.Response] = {
    val statusLine = s"Server returned ${response.status} ${response.statusText}".trim
    response
      .text()
      .toFuture
      .map { body =>
        io.circe.parser
          .parse(body)
          .toOption
          .flatMap(_.hcursor.downField("error").downField("message").as[String].toOption)
          .filter(_.nonEmpty)
          .getOrElse(statusLine)
      }
      .recover { case _ => statusLine }
      .flatMap(msg => Future.failed(new RuntimeException(msg)))
  }

  /** Fetch and decode a V2 API response, respecting the base URL from [[ClientRoutes]]. */
  def fetchV2[A: Decoder](path: String, routes: ClientRoutes): Future[A] = {
    val init = new dom.RequestInit {
      this.method = dom.HttpMethod.GET
    }
    requestV2(path, routes, init).flatMap { response =>
      response.text().toFuture.flatMap { body =>
        io.circe.parser.decode[A](body) match {
          case Right(value) => Future.successful(value)
          case Left(_) => Future.failed(new RuntimeException("Unexpected response from server"))
        }
      }
    }
  }

  /** PUT a JSON body to a V2 API endpoint. Completes with Unit on any 2xx response
    * (the V2 PUT endpoints we target return 204 No Content).
    */
  def putV2[A: Encoder](path: String, payload: A, routes: ClientRoutes): Future[Unit] = {
    val reqHeaders = new dom.Headers()
    reqHeaders.set("Content-Type", "application/json")
    reqHeaders.set("Accept", "application/json")
    val encoded = payload.asJson.noSpaces
    val init = new dom.RequestInit {
      this.method = dom.HttpMethod.PUT
      this.headers = reqHeaders
      this.body = encoded
    }
    requestV2(path, routes, init).map(_ => ())
  }

  /** POST a JSON body to a V2 API endpoint and decode the response. */
  def postV2[A: Encoder, B: Decoder](path: String, payload: A, routes: ClientRoutes): Future[B] =
    sendJson(dom.HttpMethod.POST, path, Some(payload.asJson.noSpaces), routes).flatMap(decodeBody[B])

  /** POST with no request body, for the AIP-136 custom verbs (`…:cancel`) whose whole input is
    * the URL.
    */
  def postV2NoBody[B: Decoder](path: String, routes: ClientRoutes): Future[B] =
    sendJson(dom.HttpMethod.POST, path, None, routes).flatMap(decodeBody[B])

  /** DELETE a V2 resource. Completes on any 2xx; the response body is discarded (the V2
    * DELETEs we target echo back the deleted resource, which no caller needs).
    */
  def deleteV2(path: String, routes: ClientRoutes): Future[Unit] =
    sendJson(dom.HttpMethod.DELETE, path, None, routes).map(_ => ())

  /** Shared JSON-request plumbing for the body-carrying verbs.
    *
    * The two `RequestInit`s are built separately, each with nothing but direct field
    * assignments, rather than one built conditionally. Inside `new dom.RequestInit { … }` the
    * enclosing scope's names are shadowed by the trait's own members — a `payload` referenced
    * there would silently resolve to `RequestInit.body`, not to anything here — and Scala.js
    * lowers the block to an object literal, so only plain assignments belong in it. Both
    * failures are silent at compile time and produce a request with no body.
    */
  private def sendJson(
    httpMethod: dom.HttpMethod,
    path: String,
    payload: Option[String],
    routes: ClientRoutes,
  ): Future[dom.Response] = {
    val reqHeaders = new dom.Headers()
    jsonHeaders(hasBody = payload.isDefined).foreach { case (name, value) => reqHeaders.set(name, value) }
    requestV2(path, routes, jsonRequestInit(httpMethod, payload, reqHeaders))
  }

  /** Headers for a JSON request. `Accept` always, so a failure comes back as the AIP-193 error
    * envelope [[failWithServerMessage]] reads rather than as an HTML error page; `Content-Type`
    * only when there is a body to describe.
    */
  private[util] def jsonHeaders(hasBody: Boolean): Seq[(String, String)] =
    Seq("Accept" -> "application/json") ++ Option.when(hasBody)("Content-Type" -> "application/json")

  /** Build the `RequestInit` for [[sendJson]].
    *
    * Split out (and taking already-built headers, so it needs no DOM globals) purely so it can
    * be asserted on — see `QuineApiClientRequestInitTest`. The failure mode here is invisible: a
    * body that never gets attached compiles fine and surfaces only as the server rejecting an
    * empty payload.
    */
  private[util] def jsonRequestInit(
    httpMethod: dom.HttpMethod,
    payload: Option[String],
    reqHeaders: dom.HeadersInit,
  ): dom.RequestInit =
    // Two literals with nothing but direct assignments, rather than one built conditionally.
    // Inside `new dom.RequestInit { … }` the trait's own members shadow the enclosing scope — a
    // `payload` mentioned there resolves to `RequestInit.body`, not to the parameter — and
    // Scala.js lowers the block to an object literal, so only plain assignments belong in it.
    payload match {
      case Some(json) =>
        new dom.RequestInit {
          this.method = httpMethod
          this.headers = reqHeaders
          this.body = json
        }
      case None =>
        new dom.RequestInit {
          this.method = httpMethod
          this.headers = reqHeaders
        }
    }

  private def decodeBody[B: Decoder](response: dom.Response): Future[B] =
    response.text().toFuture.flatMap { body =>
      io.circe.parser.decode[B](body) match {
        case Right(value) => Future.successful(value)
        case Left(_) => Future.failed(new RuntimeException("Unexpected response from server"))
      }
    }

  /** Polled feed of standing queries for the given graph namespace. */
  def standingQueries(graphName: String, routes: ClientRoutes): Feed[Seq[V2StandingQueryInfo]] =
    poll(
      fetchV2[V2Page[V2StandingQueryInfo]](s"api/v2/graph/$graphName/standingQueries", routes).map(_.items),
      StreamsPollIntervalMs,
    )

  /** Polled feed of saved tap queries for the given graph namespace. */
  def graphFeeds(graphName: String, routes: ClientRoutes): Feed[Vector[V2GraphFeed]] =
    poll(fetchV2[Vector[V2GraphFeed]](s"api/v2/graph/$graphName/queryUi/graphFeeds", routes))

  /** Polled feed of ingest streams for the given graph namespace. */
  def ingestStreams(graphName: String, routes: ClientRoutes): Feed[Seq[V2IngestInfo]] =
    poll(
      fetchV2[V2Page[V2IngestInfo]](s"api/v2/graph/$graphName/ingests", routes).map(_.items),
      StreamsPollIntervalMs,
    )

  /** Polled feed of background-query executions for the given graph namespace.
    *
    * Note the bare array: unlike the standing-query and ingest lists, this endpoint does not
    * wrap its results in the AIP-158 `V2Page` envelope.
    */
  def backgroundQueries(graphName: String, routes: ClientRoutes): Feed[Seq[V2BackgroundQueryStatus]] =
    poll(
      fetchV2[Seq[V2BackgroundQueryStatus]](V2Paths.backgroundQueries(graphName), routes),
      BackgroundQueryPollIntervalMs,
    )

  /** Start a background query in `graphName`, returning the new execution's id. `body` is the
    * assembled `BackgroundQueryDef` — note the server decodes it strictly, so an unknown key
    * is a 400 rather than an ignored field.
    */
  def runBackgroundQuery(graphName: String, body: Json, routes: ClientRoutes): Future[V2BackgroundQueryCreated] =
    postV2[Json, V2BackgroundQueryCreated](V2Paths.backgroundQueries(graphName), body, routes)

  /** Ask the cluster to cancel an execution. The returned record is read at request time, so it
    * may still say `started` — the transition lands on a later poll.
    */
  def cancelBackgroundQuery(graphName: String, id: String, routes: ClientRoutes): Future[V2BackgroundQueryStatus] =
    postV2NoBody[V2BackgroundQueryStatus](V2Paths.cancelBackgroundQuery(graphName, id), routes)

  /** Delete an execution's status record. If it is still running the server cancels it first,
    * then drops the record. The response body (the record as it was) is ignored.
    */
  def deleteBackgroundQuery(graphName: String, id: String, routes: ClientRoutes): Future[Unit] =
    deleteV2(V2Paths.backgroundQuery(graphName, id), routes)

  /** Polled feed of scheduled jobs. Cluster-wide, not per-graph, and a bare array. */
  def jobs(routes: ClientRoutes): Feed[Seq[V2JobStatus]] =
    poll(fetchV2[Seq[V2JobStatus]](V2Paths.jobs, routes), StreamsPollIntervalMs)

  /** Polled feed of cluster status. Fetch failures (including the endpoint being absent,
    * e.g. single-node OSS) flow to the errors stream.
    */
  def clusterStatus(routes: ClientRoutes): Feed[V2ServiceStatus] =
    poll(fetchV2[V2ServiceStatus]("api/v2/system/status", routes))

  /** Polled feed of the system metrics report, optionally from a specific cluster member
    * (sent as the `Quine-Member-Idx` header; `None` reads the serving member). A missing
    * report (404/no content) is an empty report, not an error. The V1 twin (`useV2Api =
    * false`) has no member routing and always reads the serving member.
    */
  def metrics(member: Option[Int], routes: ClientRoutes, useV2Api: Boolean): Feed[MetricsReport] =
    poll(
      if (useV2Api)
        routes.metricsV2(member.map(_.toString)).future.map {
          case Right(Some(metrics)) => metrics
          case Right(None) => MetricsReport.empty
          case Left(clientErrors) => throw new RuntimeException(clientErrors.errors.mkString("; "))
        }
      else routes.metrics(()).future,
      MetricsPollIntervalMs,
    )

  /** Polled feed of per-shard in-memory limits, optionally from a specific cluster member
    * (`Quine-Member-Idx` header; `None` reads the serving member). The V1 twin (`useV2Api =
    * false`) has no member routing and always reads the serving member.
    */
  def shardSizeLimits(
    member: Option[Int],
    routes: ClientRoutes,
    useV2Api: Boolean,
  ): Feed[Map[Int, ShardInMemoryLimit]] =
    poll(
      if (useV2Api)
        routes.shardSizesV2(member.map(_.toString)).future.map {
          case Right(Some(shardSizes)) => shardSizes
          case Right(None) => Map.empty[Int, ShardInMemoryLimit]
          case Left(clientErrors) => throw new RuntimeException(clientErrors.errors.mkString("; "))
        }
      else routes.shardSizes(Map.empty).future,
      MetricsPollIntervalMs,
    )

  /** Polled feed of the backpressure snapshot for all active pipelines, one entry per
    * reachable cluster member (a single-entry array on an unclustered/OSS instance).
    */
  def backpressure(routes: ClientRoutes): Feed[Seq[V2BackpressureSnapshot]] =
    poll(fetchV2[Seq[V2BackpressureSnapshot]]("api/v2/system/backpressure", routes))

  /** Polled feed of saved sample queries; `useV2Api = false` reads the V1 twin route. */
  def sampleQueries(routes: ClientRoutes, useV2Api: Boolean): Feed[Vector[SampleQuery]] =
    poll(
      if (useV2Api) routes.queryUiSampleQueriesV2(()).future
      else routes.queryUiSampleQueries(()).future,
    )

  /** Polled feed of quick queries with their node predicates, in the v1 shape. The v1 type is
    * the app-side model because it is a strict superset of V2's (it carries `queryLanguage`;
    * the V2 API is Cypher-only), so a v1 Gremlin entry keeps its language across the app. A V2
    * read is converted at the wire boundary, tagged Cypher.
    */
  def quickQueries(routes: ClientRoutes, useV2Api: Boolean): Feed[Vector[UiNodeQuickQuery]] =
    poll(
      if (useV2Api)
        routes.queryUiQuickQueriesV2(()).future.map(_.map(QuickQueryConversions.v2ToV1(_, QueryLanguage.Cypher)))
      else routes.queryUiQuickQueries(()).future,
    )

  /** Polled feed of node appearance rules; `useV2Api = false` reads the V1 twin route. */
  def nodeAppearances(routes: ClientRoutes, useV2Api: Boolean): Feed[Vector[UiNodeAppearance]] =
    poll(
      if (useV2Api) routes.queryUiAppearanceV2(()).future
      else routes.queryUiAppearance(()).future,
    )

  /** Persist the full sample-query list (whole-list replace); `useV2Api = false` writes the
    * V1 twin route.
    */
  def saveSampleQueries(sampleQueries: Vector[SampleQuery], routes: ClientRoutes, useV2Api: Boolean): Future[Unit] =
    (if (useV2Api) routes.updateQueryUiSampleQueriesV2(sampleQueries).future
     else routes.updateQueryUiSampleQueries(sampleQueries).future).map(_ => ())

  /** Persist the full quick-query list; a V2 write is converted from the v1 shape at the wire
    * boundary (dropping `queryLanguage`, which the V2 API doesn't model).
    */
  def saveQuickQueries(
    quickQueries: Vector[UiNodeQuickQuery],
    routes: ClientRoutes,
    useV2Api: Boolean,
  ): Future[Unit] =
    (if (useV2Api) routes.updateQueryUiQuickQueriesV2(quickQueries.map(QuickQueryConversions.v1ToV2)).future
     else routes.updateQueryUiQuickQueries(quickQueries).future).map(_ => ())

  /** Persist the full node-appearance list; `useV2Api = false` writes the V1 twin route. */
  def saveNodeAppearances(
    appearances: Vector[UiNodeAppearance],
    routes: ClientRoutes,
    useV2Api: Boolean,
  ): Future[Unit] =
    (if (useV2Api) routes.updateQueryUiAppearanceV2(appearances).future
     else routes.updateQueryUiAppearance(appearances).future).map(_ => ())

  /** Replace `namespace`'s tap-query list (V2-only endpoint — tap queries have no V1 twin).
    * The UI mirror types convert to the wire types here, at the PUT boundary.
    */
  def saveGraphFeeds(
    namespace: NamespaceParameter,
    graphFeeds: Vector[V2GraphFeed],
    routes: ClientRoutes,
  ): Future[Unit] =
    putV2(s"api/v2/graph/${namespace.namespaceId}/queryUi/graphFeeds", graphFeeds, routes)

  private def errorMessage(t: Throwable): String = {
    val raw = Option(t.getMessage).filter(_.nonEmpty)
    raw match {
      case None => "Cannot reach server"
      case Some(m) if m.contains("Failed to fetch") || m.contains("NetworkError") || m.contains("Load failed") =>
        "Cannot reach server"
      case Some(m) => m
    }
  }
}
