package com.thatdot.quine.webapp.components.landing

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes._

/** Landing page component providing an at-a-glance dashboard.
  *
  * Composes the backpressure diagram, cluster health card (enterprise), host metrics card,
  * and any extra cards into a responsive grid layout.
  *
  * Card visibility is gated by the signed-in user's permissions.
  * `userPermissions = None` disables gating entirely (OSS, where no auth is configured).
  *
  * `clusterStatusSignal` is optional: OSS deployments should pass `None` so the
  * Cluster Health card is hidden entirely. Enterprise passes the service's signal.
  */
object LandingPage {

  /** True when no auth is configured (`None` — OSS) or when `perms` covers `needed`. */
  private def hasPermissions(perms: Option[Set[String]], needed: Set[String]): Boolean =
    perms match {
      case None => true
      case Some(granted) => needed subsetOf granted
    }

  def apply(
    metricsSignal: Signal[Pot[HostMetricsCard.MetricsData]],
    backpressureSignal: Signal[Pot[V2BackpressureSnapshot]],
    clusterStatusSignal: Option[Signal[Pot[V2ServiceStatus]]] = None,
    extraCards: Seq[(Set[String], HtmlElement)] = Seq.empty,
    userPermissions: Option[Set[String]] = None,
  ): HtmlElement = {
    def allowed(needed: Set[String]): Boolean = hasPermissions(userPermissions, needed)

    val canSeeClusterHealth = clusterStatusSignal.isDefined && allowed(ClusterHealthCard.requiredPermissions)
    val canSeeHostMetrics = allowed(HostMetricsCard.requiredPermissions)
    val canSeeBackpressure = allowed(Set("ApplicationMetricsRead"))

    val row3: Seq[HtmlElement] = (canSeeClusterHealth, canSeeHostMetrics, clusterStatusSignal) match {
      case (true, true, Some(sig)) =>
        Seq(
          div(cls := "col-12 col-md-6 mt-3", ClusterHealthCard(sig)),
          div(cls := "col-12 col-md-6 mt-3", HostMetricsCard(metricsSignal)),
        )
      case (true, false, Some(sig)) =>
        Seq(div(cls := "col-12 mt-3", ClusterHealthCard(sig)))
      case (false, true, _) =>
        Seq(div(cls := "col-12 mt-3", HostMetricsCard(metricsSignal)))
      case _ =>
        Seq.empty
    }

    val allowedExtras: Seq[HtmlElement] = extraCards.collect {
      case (needed, card) if allowed(needed) => card
    }

    div(
      onUnmountCallback(_ => LandingTooltip.hide()),
      // Blue toolbar
      div(
        cls := s"${Styles.navBar} d-flex align-items-center px-3",
        span(
          cls := s"${Styles.navBarButton}",
          styleAttr := "font-size: 1.4em; font-weight: 600; padding-left: 0;",
          "Dashboard",
        ),
        div(
          cls := "d-flex align-items-center ms-auto me-3",
          // Derive ingest and SQ counts from the backpressure snapshot
          if (canSeeBackpressure)
            child <-- backpressureSignal.map { pot =>
              val snapOpt = pot.toOption
              val ingestCount = snapOpt.map(_.ingests.size).getOrElse(0)
              val sqCount = snapOpt.map(_.standingQueries.size).getOrElse(0)
              span(
                cls := "d-inline-flex align-items-center",
                summaryBadge(ingestCount, if (ingestCount == 1) "ingest" else "ingests"),
                summaryBadge(sqCount, if (sqCount == 1) "query" else "queries"),
              )
            }
          else emptyNode,
          // Cluster host count (enterprise only)
          if (canSeeClusterHealth)
            child <-- clusterStatusSignal
              .getOrElse(Signal.fromValue(Pot.Empty: Pot[V2ServiceStatus]))
              .map { p =>
                val count = p.toOption.map(s => s.cluster.clusterMembers.size + s.cluster.hotSpares.size).getOrElse(0)
                summaryBadge(count, if (count == 1) "host" else "hosts")
              }
          else emptyNode,
        ),
      ),
      // Card grid
      div(
        cls := "container-fluid",
        // Backpressure & throughput diagram
        if (canSeeBackpressure)
          div(
            cls := "row px-3",
            div(
              cls := "col-12 mt-3",
              div(
                cls := "card h-100",
                styleAttr := "background:#f4f5fa;border:1px solid rgba(10,41,91,0.1);border-radius:14px;padding:6px 8px;box-shadow:0 6px 22px rgba(10,41,91,0.06);",
                BackpressureDiagram(backpressureSignal, clusterStatusSignal),
              ),
            ),
          )
        else emptyNode,
        // Cluster Health (enterprise only) + Host Metrics
        if (row3.nonEmpty) div(cls := "row px-3", row3) else emptyNode,
        // Extra cards (e.g., License Usage for enterprise)
        allowedExtras.map { card =>
          div(cls := "row px-3", div(cls := "col-12 col-md-6 mt-3", card))
        },
      ),
    )
  }

  private def summaryBadge(count: Int, label: String): HtmlElement =
    span(
      cls := "d-inline-flex align-items-center me-3",
      styleAttr := "color: var(--thatdot-gradient-end); font-size: 1em;",
      span(
        cls := "badge",
        styleAttr := "background-color: rgba(255,255,255,0.2); margin-right: 0.4em;",
        count.toString,
      ),
      label,
    )
}
