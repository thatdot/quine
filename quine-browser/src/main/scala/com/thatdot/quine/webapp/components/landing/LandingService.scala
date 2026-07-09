package com.thatdot.quine.webapp.components.landing

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes.{ClientRoutes, MetricsReport, ShardInMemoryLimit}
import com.thatdot.quine.webapp.util.QuineApiClient
import com.thatdot.quine.webapp.v2api.V2ApiTypes._

/** Server-connection capability for the landing page.
  *
  * Each datum is exposed as a pair of streams: one that emits successful values and one
  * that emits failure messages. Internally each pair is driven by a single poll loop that
  * re-fetches the underlying endpoint every [[LandingService.PollIntervalMs]] ms. Splitting
  * success and failure lets the store keep "last good" data unchanged on a failed tick.
  */
final class LandingService(routes: ClientRoutes, fetchShardSizes: Boolean = true) {

  type MetricsData = (MetricsReport, Map[Int, ShardInMemoryLimit])

  val metrics: LandingService.Feed[MetricsData] = poll(fetchMetrics())
  val clusterStatus: LandingService.Feed[V2ServiceStatus] = poll(fetchClusterStatus())
  val backpressure: LandingService.Feed[V2BackpressureSnapshot] = poll(fetchBackpressure())

  private def poll[A](fetch: => Future[A]): LandingService.Feed[A] = {
    val inner = QuineApiClient.poll(fetch)
    LandingService.Feed(values = inner.values, errors = inner.errors)
  }

  private def fetchMetrics(): Future[MetricsData] = {
    // The landing page wants one health snapshot; the request resolves to the serving member.
    val metricsF = routes.metricsV2(None).future.map {
      case Right(Some(metrics)) => metrics
      case Right(None) => MetricsReport.empty
      case Left(err) => throw new RuntimeException(s"Failed to get metrics: $err")
    }
    val shardSizesF: Future[Map[Int, ShardInMemoryLimit]] =
      if (!fetchShardSizes) Future.successful(Map.empty[Int, ShardInMemoryLimit])
      else
        routes.shardSizesV2(None).future.map {
          case Right(Some(shardSizes)) => shardSizes
          case Right(None) => Map.empty[Int, ShardInMemoryLimit]
          case Left(err) => throw new RuntimeException(s"Failed to get shard sizes: $err")
        }
    metricsF.zip(shardSizesF)
  }

  /** Enterprise-only: fetch cluster status (members + hot spares). */
  private def fetchClusterStatus(): Future[V2ServiceStatus] =
    QuineApiClient.fetchV2[V2ServiceStatus]("api/v2/system/status", routes)

  /** Fetch the backpressure snapshot for all active pipelines. */
  private def fetchBackpressure(): Future[V2BackpressureSnapshot] =
    QuineApiClient.fetchV2[Seq[V2BackpressureSnapshot]]("api/v2/system/backpressure", routes).map { snapshots =>
      snapshots.maxBy(_.timestamp)
    }
}

object LandingService {
  val PollIntervalMs: Int = 5000

  /** A polled feed: successful values and failure messages on separate streams. */
  final case class Feed[A](values: EventStream[A], errors: EventStream[String])
}
