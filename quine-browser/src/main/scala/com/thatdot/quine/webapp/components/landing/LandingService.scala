package com.thatdot.quine.webapp.components.landing

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import org.scalajs.dom
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes.{ClientRoutes, MetricsReport, ShardInMemoryLimit}
import com.thatdot.quine.webapp.components.landing.V2ApiTypes._
import com.thatdot.quine.webapp.util.PollingStream

/** Server-connection capability for the landing page.
  *
  * Each datum is exposed as a pair of streams: one that emits successful values and one
  * that emits failure messages. Internally each pair is driven by a single poll loop that
  * re-fetches the underlying endpoint every [[LandingService.PollIntervalMs]] ms. Splitting
  * success and failure lets the store keep "last good" data unchanged on a failed tick.
  *
  * Metrics use the existing endpoints4s V2 routes via ClientRoutes. Ingests and standing
  * queries use dom.fetch against the V2 REST API directly, since there are no endpoints4s
  * traits for these V2 endpoints.
  */
final class LandingService(routes: ClientRoutes, fetchShardSizes: Boolean = true) {

  type MetricsData = (MetricsReport, Map[Int, ShardInMemoryLimit])

  val metrics: LandingService.Feed[MetricsData] = poll(fetchMetrics())
  val ingests: LandingService.Feed[Seq[V2IngestInfo]] = poll(fetchIngests())
  val standingQueries: LandingService.Feed[Seq[V2StandingQueryInfo]] = poll(fetchStandingQueries())
  val clusterStatus: LandingService.Feed[V2ServiceStatus] = poll(fetchClusterStatus())
  val config: LandingService.Feed[V2QuineConfig] = poll(fetchConfig())

  private def poll[A](fetch: => Future[A]): LandingService.Feed[A] = {
    val ticks = PollingStream(LandingService.PollIntervalMs) {
      fetch.transform(scala.util.Success(_))
    }
    LandingService.Feed(
      values = ticks.collect { case scala.util.Success(value) => value },
      errors = ticks.collect { case scala.util.Failure(t) => errorMessage(t) },
    )
  }

  private def errorMessage(t: Throwable): String = {
    val raw = Option(t.getMessage).filter(_.nonEmpty)
    raw match {
      case None => "Cannot reach server"
      // Browsers surface network-layer failures (server down, DNS, CORS, offline)
      // as "Failed to fetch" / "NetworkError" / "Load failed". Normalize those.
      case Some(m) if m.contains("Failed to fetch") || m.contains("NetworkError") || m.contains("Load failed") =>
        "Cannot reach server"
      case Some(m) => m
    }
  }

  private def fetchMetrics(): Future[MetricsData] = {
    val metricsF = routes.metricsV2(()).future.map {
      case Right(Some(metrics)) => metrics
      case Right(None) => MetricsReport.empty
      case Left(err) => throw new RuntimeException(s"Failed to get metrics: $err")
    }
    // Only callers that hold `ShardLimitsRead` should fetch shard sizes — admin holds
    // `ApplicationMetricsRead` (so the persistor lane wants metrics) but not
    // `ShardLimitsRead`, and a 401 on shardSizes would otherwise fail the whole zip
    // and starve the persistor animation of write/read rates.
    val shardSizesF: Future[Map[Int, ShardInMemoryLimit]] =
      if (!fetchShardSizes) Future.successful(Map.empty[Int, ShardInMemoryLimit])
      else
        routes.shardSizesV2(()).future.map {
          case Right(Some(shardSizes)) => shardSizes
          case Right(None) => Map.empty[Int, ShardInMemoryLimit]
          case Left(err) => throw new RuntimeException(s"Failed to get shard sizes: $err")
        }
    metricsF.zip(shardSizesF)
  }

  // OSS endpoints are scoped to the literal `quine` graph. Enterprise's landing page
  // doesn't surface ingests or standing queries.
  private def fetchIngests(): Future[Seq[V2IngestInfo]] =
    fetchV2[V2Page[V2IngestInfo]]("api/v2/graph/quine/ingests").map(_.items)

  private def fetchStandingQueries(): Future[Seq[V2StandingQueryInfo]] =
    fetchV2[V2Page[V2StandingQueryInfo]]("api/v2/graph/quine/standingQueries").map(_.items)

  /** Enterprise-only: fetch cluster status (members + hot spares). */
  private def fetchClusterStatus(): Future[V2ServiceStatus] =
    fetchV2[V2ServiceStatus]("api/v2/system/status")

  /** Fetch the running config and extract the persistor store type. */
  private def fetchConfig(): Future[V2QuineConfig] =
    fetchV2[V2QuineConfig]("api/v2/system/config")

  /** Fetch from a V2 API endpoint, honoring the same base URL that endpoints4s uses
    * via `ClientRoutes.baseUrlOpt`. When the base URL is set (typically derived by the
    * startup JS to absorb any reverse-proxy path prefix), it's prepended; otherwise
    * the path is left relative so the browser resolves it against the current
    * document — which preserves the proxy prefix automatically.
    */
  private def fetchV2[A: io.circe.Decoder](path: String): Future[A] = {
    val url = routes.baseUrlOpt match {
      case Some(base) => s"${base.stripSuffix("/")}/$path"
      case None => path
    }
    dom
      .fetch(url)
      .toFuture
      .recoverWith { case _ =>
        Future.failed(new RuntimeException("Cannot reach server"))
      }
      .flatMap { response =>
        if (!response.ok) {
          Future.failed(new RuntimeException(s"Server returned ${response.status} ${response.statusText}"))
        } else {
          response.text().toFuture.flatMap { body =>
            io.circe.parser.decode[A](body) match {
              case Right(value) => Future.successful(value)
              case Left(_) => Future.failed(new RuntimeException("Unexpected response from server"))
            }
          }
        }
      }
  }
}

object LandingService {
  val PollIntervalMs: Int = 5000

  /** A polled feed: successful values and failure messages on separate streams. */
  final case class Feed[A](values: EventStream[A], errors: EventStream[String])
}
