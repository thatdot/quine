package com.thatdot.quine.webapp.components.landing

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.components.dashboard.Card
import com.thatdot.quine.webapp.components.landing.V2ApiTypes.{V2QuineHost, V2ServiceStatus}
import com.thatdot.quine.webapp.util.Pot

/** Card displaying cluster host health from the V2 admin status endpoint.
  *
  * Enterprise-only: the OSS server does not expose `/api/v2/admin/status`,
  * so the card is only mounted by the enterprise landing view.
  */
object ClusterHealthCard {

  val requiredPermissions: Set[String] = Set("ClusterStatusRead")

  def apply(statusSignal: Signal[Pot[V2ServiceStatus]]): HtmlElement =
    div(child <-- statusSignal.map(renderState))

  private def renderState(state: Pot[V2ServiceStatus]): HtmlElement = state match {
    case Pot.Empty =>
      Card(title = "Cluster Status", body = p(cls := "text-muted mb-0", "No cluster data loaded."))

    case Pot.Pending =>
      Card(
        title = "Cluster Status",
        body = div(
          cls := "d-flex align-items-center",
          span(cls := "spinner-border spinner-border-sm me-2"),
          span("Loading cluster status..."),
        ),
      )

    case Pot.Ready(status) =>
      renderContent(status)

    case Pot.Failed(err) =>
      Card(title = "Cluster Status", body = div(cls := "alert alert-danger mb-0", s"Error: $err"))

    // PendingStale isn't currently produced by LandingStore; render like Ready if it ever is.
    case Pot.PendingStale(status) =>
      renderContent(status)

    // FailedStale: keep showing the prior data; a single page-level "Failed to refresh"
    // banner in LandingPage covers the messaging across all cards.
    case Pot.FailedStale(status, _) =>
      renderContent(status)
  }

  private def renderContent(status: V2ServiceStatus): HtmlElement = {
    val members = status.cluster.clusterMembers.toSeq.sortBy { case (idx, _) =>
      scala.util.Try(idx.toInt).getOrElse(Int.MaxValue)
    }
    val spares = status.cluster.hotSpares

    Card(
      title = "Cluster Status",
      body = div(
        styleAttr := "font-size: 1rem;",
        div(
          cls := "d-flex justify-content-between align-items-center mb-2",
          span(
            cls := "text-muted",
            styleAttr := "cursor: pointer;",
            s"target size: ${status.cluster.targetSize}",
            LandingTooltip.attach[com.raquo.laminar.nodes.ReactiveHtmlElement.Base](buildTargetSizeTooltip(status)),
          ),
          overallBadge(status),
        ),
        table(
          cls := "table table-hover mb-0",
          thead(
            tr(
              th("Role"),
              th("Address"),
            ),
          ),
          tbody(
            members.map { case (idx, host) => memberRow(s"member-$idx", host) } ++
            spares.zipWithIndex.map { case (host, i) => memberRow(s"spare-$i", host, isSpare = true) },
          ),
        ),
      ),
    )
  }

  private def memberRow(label: String, host: V2QuineHost, isSpare: Boolean = false): HtmlElement =
    tr(
      styleAttr := "cursor: pointer;",
      LandingTooltip.attach[com.raquo.laminar.nodes.ReactiveHtmlElement.Base](
        buildHostTooltip(label, host, isSpare),
      ),
      td(
        span(
          cls := s"badge ${if (isSpare) "bg-secondary" else "bg-primary"} me-1",
          styleAttr := "font-size: 0.85em;",
          if (isSpare) "spare" else "active",
        ),
        label,
      ),
      td(s"${host.address}:${host.port}"),
    )

  private def overallBadge(status: V2ServiceStatus): HtmlElement = {
    val span0 =
      if (status.fullyUp) span(cls := "badge bg-success", styleAttr := "cursor: pointer;", "Fully Up")
      else span(cls := "badge bg-danger", styleAttr := "cursor: pointer;", "Degraded")
    span0.amend(
      LandingTooltip.attach[com.raquo.laminar.nodes.ReactiveHtmlElement.Base](
        buildOverallTooltip(status),
      ),
    )
  }

  private def buildHostTooltip(label: String, host: V2QuineHost, isSpare: Boolean): String = {
    val role = if (isSpare) "Hot Spare" else "Cluster Member"
    LandingTooltip.header(label) +
    LandingTooltip.kvTable(
      LandingTooltip.kvRow("Role", role) +
      LandingTooltip.kvRow("Address", host.address) +
      LandingTooltip.kvRow("Port", host.port.toString) +
      LandingTooltip.kvRow("UID", host.uid.toString),
    )
  }

  private def buildOverallTooltip(status: V2ServiceStatus): String = {
    val activeCount = status.cluster.clusterMembers.size
    val spareCount = status.cluster.hotSpares.size
    val target = status.cluster.targetSize
    val body = LandingTooltip.kvTable(
      LandingTooltip.kvRow(
        "Status",
        if (status.fullyUp) "Fully Up" else "Degraded",
        Some(if (status.fullyUp) LandingTooltip.statusColor("Running") else LandingTooltip.statusColor("Failed")),
      ) +
      LandingTooltip.kvRow("Active members", s"$activeCount / $target") +
      LandingTooltip.kvRow("Hot spares", s"$spareCount"),
    )
    val breakdown =
      if (activeCount < target) {
        val missing = target - activeCount
        LandingTooltip.subheader("Capacity") +
        s"""<div style="font-size: 13px; color: #acacc9;">Missing $missing of $target members.</div>"""
      } else ""
    LandingTooltip.header("Cluster status") + body + breakdown
  }

  private def buildTargetSizeTooltip(status: V2ServiceStatus): String = {
    val active = status.cluster.clusterMembers.size
    val spares = status.cluster.hotSpares.size
    LandingTooltip.header("Cluster sizing") +
    LandingTooltip.kvTable(
      LandingTooltip.kvRow("Target size", status.cluster.targetSize.toString) +
      LandingTooltip.kvRow("Active", s"$active") +
      LandingTooltip.kvRow("Hot spares", s"$spares"),
    )
  }
}
