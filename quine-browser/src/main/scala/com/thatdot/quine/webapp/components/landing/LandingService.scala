package com.thatdot.quine.webapp.components.landing

import scala.concurrent.Future

import org.scalajs.dom
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes.{ClientRoutes, MetricsReport, ShardInMemoryLimit}
import com.thatdot.quine.webapp.components.landing.V2ApiTypes._

/** Server-connection capability for the landing page.
  *
  * Pure data-fetching layer. Returns Futures — no reactive state, no command processing.
  * The store calls this and manages the Pot lifecycle.
  *
  * Metrics use the existing endpoints4s V2 routes via ClientRoutes.
  * Ingests and standing queries use dom.fetch against the V2 REST API directly,
  * since there are no endpoints4s traits for these V2 endpoints.
  */
final class LandingService(routes: ClientRoutes) {

  type MetricsData = (MetricsReport, Map[Int, ShardInMemoryLimit])

  def fetchMetrics(): Future[MetricsData] = {
    val metricsF = routes.metricsV2(()).future.map {
      case Right(Some(metrics)) => metrics
      case Right(None) => MetricsReport.empty
      case Left(err) => throw new RuntimeException(s"Failed to get metrics: $err")
    }
    val shardSizesF = routes.shardSizesV2(()).future.map {
      case Right(Some(shardSizes)) => shardSizes
      case Right(None) => Map.empty[Int, ShardInMemoryLimit]
      case Left(err) => throw new RuntimeException(s"Failed to get shard sizes: $err")
    }
    metricsF.zip(shardSizesF)
  }

  def fetchIngests(): Future[Seq[V2IngestInfo]] =
    fetchV2[Seq[V2IngestInfo]]("api/v2/ingests")

  def fetchStandingQueries(): Future[Seq[V2StandingQueryInfo]] =
    fetchV2[Seq[V2StandingQueryInfo]]("api/v2/standing-queries")

  /** Enterprise-only: fetch cluster status (members + hot spares). */
  def fetchClusterStatus(): Future[V2ServiceStatus] =
    fetchV2[V2ServiceStatus]("api/v2/admin/status")

  /** Fetch the running config and extract the persistor store type. */
  def fetchConfig(): Future[V2QuineConfig] =
    fetchV2[V2QuineConfig]("api/v2/admin/config")

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
      .flatMap(_.text().toFuture)
      .flatMap { body =>
        io.circe.parser.decode[V2Response[A]](body) match {
          case Right(response) => Future.successful(response.content)
          case Left(err) => Future.failed(new RuntimeException(s"Failed to decode $path: ${err.getMessage}"))
        }
      }
  }
}
