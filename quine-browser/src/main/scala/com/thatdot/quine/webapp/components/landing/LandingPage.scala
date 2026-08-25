package com.thatdot.quine.webapp.components.landing

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.dataservice.BackpressureService
import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes._

/** Landing page component providing an at-a-glance dashboard.
  *
  * Composes the backpressure diagram across the top, then a row sharing any extra cards
  * (e.g. License Usage) with the host metrics card. That bottom row lays its cards out
  * side by side, or full width when only one of them is visible.
  *
  * Card visibility is gated by the signed-in user's permissions.
  * `userPermissions = None` disables gating entirely (OSS, where no auth is configured).
  *
  * `clusterStatusSignal` is optional: OSS deployments should pass `None`, which hides the
  * cluster host count in the toolbar and leaves the backpressure diagram without cluster
  * context. Enterprise passes the service's signal.
  */
object LandingPage {

  /** Permission required to read cluster membership (host count badge, backpressure host breakdown). */
  private val clusterStatusPermissions: Set[String] = Set("ClusterStatusRead")

  /** True when no auth is configured (`None` — OSS) or when `perms` covers `needed`. */
  private def hasPermissions(perms: Option[Set[String]], needed: Set[String]): Boolean =
    perms match {
      case None => true
      case Some(granted) => needed subsetOf granted
    }

  def apply(
    metricsSignal: Signal[Pot[HostMetricsCard.MetricsData]],
    backpressureService: BackpressureService,
    clusterStatusSignal: Option[Signal[Pot[V2ServiceStatus]]] = None,
    extraCards: Seq[(Set[String], HtmlElement)] = Seq.empty,
    userPermissions: Option[Set[String]] = None,
    showScopePicker: Boolean = false,
  ): HtmlElement = {
    def allowed(needed: Set[String]): Boolean = hasPermissions(userPermissions, needed)

    val canSeeClusterStatus = clusterStatusSignal.isDefined && allowed(clusterStatusPermissions)
    val canSeeHostMetrics = allowed(HostMetricsCard.requiredPermissions)
    val canSeeBackpressure = allowed(Set("ApplicationMetricsRead"))

    val allowedExtras: Seq[HtmlElement] = extraCards.collect {
      case (needed, card) if allowed(needed) => card
    }

    // Extras and host metrics share the bottom row. A lone card takes the full width rather
    // than leaving half the row empty; two or more sit side by side and wrap past that.
    val bottomRow: Seq[HtmlElement] =
      allowedExtras ++ (if (canSeeHostMetrics) Seq(HostMetricsCard(metricsSignal)) else Seq.empty)
    val bottomRowColumn = if (bottomRow.size > 1) "col-12 col-md-6 mt-3" else "col-12 mt-3"

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
        // Thin divider between the page title and the summary chips
        span(styleAttr := "width:1px;height:20px;background:rgba(255,255,255,0.25);margin:0 18px;flex:0 0 auto;"),
        div(
          cls := "d-flex align-items-center",
          // Derive ingest and SQ counts from the view. Both lists are already resolved across the
          // cluster — ingests unioned (each lives on one host), standing queries merged by name
          // (they run on every host) — so these are plain sizes rather than a dedup at the call site.
          if (canSeeBackpressure)
            child <-- backpressureService.backpressureSnapshotSignal.map { pot =>
              val ingestCount = pot.toOption.map(_.ingests.size).getOrElse(0)
              val sqCount = pot.toOption.map(_.standingQueries.size).getOrElse(0)
              span(
                cls := "d-inline-flex align-items-center",
                summaryBadge(ingestCount, if (ingestCount == 1) "ingest" else "ingests"),
                summaryBadge(sqCount, if (sqCount == 1) "query" else "queries"),
              )
            }
          else emptyNode,
          // Cluster host count (enterprise only)
          if (canSeeClusterStatus)
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
                // Only pass cluster status to the diagram when the user may read it. The signal can
                // be populated with a trimmed, member-positions-only status for ingest-capable roles
                // (to drive the Streams host selector); those roles must not see cluster detail here.
                BackpressureDiagram(
                  backpressureService,
                  if (canSeeClusterStatus) clusterStatusSignal else None,
                  showScopePicker = showScopePicker,
                ),
              ),
            ),
          )
        else emptyNode,
        // Extra cards (e.g., License Usage for enterprise) + Host Metrics
        if (bottomRow.nonEmpty)
          div(cls := "row px-3", bottomRow.map(card => div(cls := bottomRowColumn, card)))
        else emptyNode,
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
