package com.thatdot.quine.webapp.components.dashboard

import scala.concurrent.duration.DurationInt

import com.raquo.laminar.api.L._
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes.{ClientRoutes, MetricsReport, ShardInMemoryLimit}
import com.thatdot.quine.webapp.components.dashboard.MetricsDashboardRenderer.MetricsResult
import com.thatdot.quine.webapp.queryui.QueryMethod
import com.thatdot.quine.webapp.util.PollingStream

object MetricsDashboard {

  def apply(routes: ClientRoutes, queryMethod: QueryMethod): HtmlElement = {
    val metricsStream: EventStream[MetricsResult] = PollingStream(2.seconds.toMillis.toInt) {
      val (metricsF, shardSizesF) = queryMethod match {
        case QueryMethod.RestfulV2 | QueryMethod.WebSocketV2 =>
          val metricsF = routes.metricsV2(()).future.map {
            case Right(Some(metrics)) => metrics
            case Right(None) => MetricsReport.empty
            case Left(_) => throw new RuntimeException("Failed to get metrics from V2 API")
          }
          val shardSizesF = routes.shardSizesV2(()).future.map {
            case Right(Some(shardSizes)) => shardSizes
            case Right(None) => Map.empty[Int, ShardInMemoryLimit]
            case Left(_) => throw new RuntimeException("Failed to get shard sizes from V2 API")
          }
          (metricsF, shardSizesF)

        case QueryMethod.Restful | QueryMethod.WebSocket =>
          val metricsF = routes.metrics(()).future
          val shardSizesF = routes.shardSizes(Map.empty).future
          (metricsF, shardSizesF)
      }

      metricsF
        .zip(shardSizesF)
        .map[MetricsResult](result => Right(result))
        .recover { case exception =>
          val errorMsg =
            if (exception.getMessage.isEmpty) "Failed to read metrics from server"
            else s"Failed to read metrics from server: ${exception.getMessage}"
          Left(errorMsg): MetricsResult
        }
    }

    MetricsDashboardRenderer.renderDashboard(metricsStream)
  }
}
