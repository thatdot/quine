package com.thatdot.quine.webapp.components.dashboard

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.components.dashboard.MetricsDashboardRenderer.MetricsResult
import com.thatdot.quine.webapp.dataservice.DataService
import com.thatdot.quine.webapp.util.Pot

object MetricsDashboard {

  /** @param enableMemberSelector show the cluster-position selector, fed by the service's
    *   member list. Only useful when the server exposes `/api/v2/system/status`; gating at
    *   construction means servers without it are never asked (an unbound signal never polls).
    */
  def apply(dataService: DataService, enableMemberSelector: Boolean = false): HtmlElement = {
    // Position 0 always exists, so default to it — no membership lookup is needed to pick it.
    // A single-node server ignores the position header.
    val selectedMember = Var[Option[Int]](Some(0))

    val memberIndices: Signal[Seq[Int]] =
      if (enableMemberSelector) dataService.memberIndicesSignal else Val(Seq.empty)

    // Selecting a member switches to that member's shared feed. A late response from the
    // previously selected member can't overwrite the display: its subscription is dropped
    // with the switch.
    val metricsStream: EventStream[MetricsResult] =
      selectedMember.signal
        .flatMapSwitch(sel => dataService.hostMetricsSignal(sel))
        .updates
        .collect {
          case Pot.Ready(pair) => Right(pair)
          case Pot.PendingStale(pair) => Right(pair)
          case Pot.Failed(err) => Left(err)
          case Pot.FailedStale(_, err) => Left(err)
        }

    div(
      MetricsDashboardRenderer.renderDashboard(
        metricsStream,
        belowTitle = renderMemberSelector(memberIndices, selectedMember),
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
