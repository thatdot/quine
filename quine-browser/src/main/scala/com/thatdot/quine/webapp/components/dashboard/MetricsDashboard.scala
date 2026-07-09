package com.thatdot.quine.webapp.components.dashboard

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import com.raquo.laminar.api.L._
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes.{ClientRoutes, MetricsReport, ShardInMemoryLimit}
import com.thatdot.quine.webapp.components.dashboard.MetricsDashboardRenderer.MetricsResult
import com.thatdot.quine.webapp.queryui.QueryMethod
import com.thatdot.quine.webapp.util.PollingStream
import com.thatdot.quine.webapp.v2api.SystemStatusApi

object MetricsDashboard {

  /** @param enableMemberSelector fetch the cluster member list and show the position selector.
    *   Only useful when the server exposes `/api/v2/system/status`.
    */
  def apply(routes: ClientRoutes, queryMethod: QueryMethod, enableMemberSelector: Boolean = false): HtmlElement = {
    // Position 0 always exists, so default to it — no membership lookup is needed to pick it.
    // A single-node server ignores the position header.
    val selectedMember = Var[Option[Int]](Some(0))
    val memberIndices = Var[Seq[Int]](Seq.empty)

    def fetchMetrics(member: Option[Int]): Future[MetricsResult] = {
      val (metricsF, shardSizesF) = queryMethod match {
        case QueryMethod.RestfulV2 | QueryMethod.WebSocketV2 =>
          val metricsF = routes.metricsV2(member.map(_.toString)).future.map {
            case Right(Some(metrics)) => metrics
            case Right(None) => MetricsReport.empty
            case Left(_) => throw new RuntimeException("Failed to get metrics from V2 API")
          }
          val shardSizesF = routes.shardSizesV2(member.map(_.toString)).future.map {
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

    // One poll, restarted per member selection. PollingStream fires immediately on start, so a
    // selection change fetches at once — and switching drops any in-flight response for the
    // previously selected member, which could otherwise arrive late and overwrite the display.
    val metricsStream: EventStream[MetricsResult] =
      selectedMember.signal.flatMapSwitch(sel => PollingStream(2.seconds.toMillis.toInt)(fetchMetrics(sel)))

    div(
      // The member list only populates the selector's options; the default (0) needs no lookup.
      // Servers without a `/api/v2/system/status` endpoint never populate it.
      onMountCallback { _ =>
        if (enableMemberSelector)
          SystemStatusApi.memberIndices(routes.baseUrlOpt).foreach(memberIndices.set)
      },
      MetricsDashboardRenderer.renderDashboard(
        metricsStream,
        belowTitle = renderMemberSelector(memberIndices.signal, selectedMember),
      ),
    )
  }

  /** Member picker, shown only when cluster membership is known (more than zero members
    * reported). Single-node deployments render nothing and metrics resolve to the only member.
    */
  private def renderMemberSelector(memberIndices: Signal[Seq[Int]], selectedMember: Var[Option[Int]]): HtmlElement =
    div(
      cls := "px-3",
      child <-- memberIndices.map { indices =>
        if (indices.isEmpty) emptyNode
        else
          div(
            cls := "mt-3 d-inline-flex align-items-center",
            label(cls := "form-label me-2 mb-0", "Member position"),
            select(
              cls := "form-select form-select-sm",
              styleAttr := "width: auto;",
              controlled(
                value <-- selectedMember.signal.map(_.fold("")(_.toString)),
                onChange.mapToValue.map(_.toIntOption) --> selectedMember,
              ),
              indices.sorted.map(i => option(value := i.toString, i.toString)),
            ),
          )
      },
    )
}
