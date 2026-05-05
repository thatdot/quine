package com.thatdot.quine.webapp.components.landing

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.{MetricsReport, ShardInMemoryLimit}
import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.components.landing.V2ApiTypes._
import com.thatdot.quine.webapp.util.Pot

/** Landing page component providing an at-a-glance dashboard.
  *
  * Composes individual card components into a responsive grid layout.
  * Each card manages its own rendering based on Pot lifecycle state.
  *
  * Card visibility is gated by the signed-in user's permissions: each card object
  * exposes a `requiredPermissions` set, and the page renders a card only when
  * those are a subset of `userInfo.permissions`. `userInfo = None` disables
  * gating entirely (OSS, where no auth is configured).
  *
  * `clusterStatusSignal` is optional: OSS deployments should pass `None` so the
  * Cluster Health card is hidden entirely. Enterprise passes the store's signal.
  */
object LandingPage {

  type MetricsData = (MetricsReport, Map[Int, ShardInMemoryLimit])

  /** True when no auth is configured (`None` — OSS) or when `perms` covers `needed`. */
  def hasPermissions(perms: Option[Set[String]], needed: Set[String]): Boolean =
    perms match {
      case None => true
      case Some(granted) => needed subsetOf granted
    }

  def apply(
    metricsSignal: Signal[Pot[MetricsData]],
    ingestsSignal: Signal[Pot[Seq[V2IngestInfo]]],
    standingQueriesSignal: Signal[Pot[Seq[V2StandingQueryInfo]]],
    configSignal: Signal[Pot[V2QuineConfig]],
    subscriptions: Modifier[HtmlElement],
    clusterStatusSignal: Option[Signal[Pot[V2ServiceStatus]]] = None,
    extraCards: Seq[(Set[String], HtmlElement)] = Seq.empty,
    userPermissions: Option[Set[String]] = None,
  ): HtmlElement = {
    def allowed(needed: Set[String]): Boolean = hasPermissions(userPermissions, needed)

    val canSeeOverview = allowed(SystemOverviewCard.requiredPermissions)
    val canSeeIngests = allowed(IngestsCard.requiredPermissions)
    val canSeeStandingQueries = allowed(StandingQueriesCard.requiredPermissions)
    val canSeeClusterHealth = clusterStatusSignal.isDefined && allowed(ClusterHealthCard.requiredPermissions)
    val canSeeHostMetrics = allowed(HostMetricsCard.requiredPermissions)

    val row2: Seq[HtmlElement] =
      (if (canSeeIngests) Seq(div(cls := "col-12 col-md-6 mt-3", IngestsCard(ingestsSignal))) else Seq.empty) ++
      (if (canSeeStandingQueries) Seq(div(cls := "col-12 col-md-6 mt-3", StandingQueriesCard(standingQueriesSignal)))
       else Seq.empty)

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

    // A polled feed enters `FailedStale` when a refresh fails after data has already
    // loaded. Cards keep showing the prior data; this page-level banner is the single
    // user-facing indication that the page is no longer live. Only signals whose card
    // is actually mounted are watched, so e.g. cluster-status is ignored on OSS.
    def isStale(p: Pot[_]): Boolean = p match {
      case Pot.FailedStale(_, _) => true
      case _ => false
    }
    val watchedSignals: Seq[Signal[Pot[_]]] =
      (if (canSeeIngests) Seq(ingestsSignal) else Nil) ++
      (if (canSeeStandingQueries) Seq(standingQueriesSignal) else Nil) ++
      (if (canSeeClusterHealth) clusterStatusSignal.toSeq else Nil) ++
      (if (canSeeHostMetrics) Seq(metricsSignal) else Nil) ++
      (if (canSeeOverview) Seq(configSignal) else Nil)
    val refreshFailedSignal: Signal[Boolean] =
      if (watchedSignals.isEmpty) Signal.fromValue(false)
      else Signal.combineSeq(watchedSignals).map(_.exists(isStale)).distinct

    div(
      subscriptions,
      onUnmountCallback(_ => LandingTooltip.hide()),
      // Blue toolbar matching the Exploration UI nav-bar
      div(
        cls := s"${Styles.navBar} d-flex align-items-center px-3",
        span(
          cls := s"${Styles.navBarButton}",
          styleAttr := "font-size: 1.4em; font-weight: 600; padding-left: 0;",
          "Dashboard",
        ),
        div(
          cls := "d-flex align-items-center ms-auto me-3",
          if (canSeeIngests)
            child <-- ingestsSignal.map(p =>
              summaryBadge(
                p.toOption.map(_.size).getOrElse(0),
                "ingests",
                buildIngestsSummaryTooltip(p.toOption.getOrElse(Nil)),
              ),
            )
          else emptyNode,
          if (canSeeStandingQueries)
            child <-- standingQueriesSignal.map(p =>
              summaryBadge(
                p.toOption.map(_.size).getOrElse(0),
                "queries",
                buildQueriesSummaryTooltip(p.toOption.getOrElse(Nil)),
              ),
            )
          else emptyNode,
          if (canSeeClusterHealth)
            child <-- clusterStatusSignal
              .getOrElse(Signal.fromValue(Pot.Empty: Pot[V2ServiceStatus]))
              .map(p =>
                summaryBadge(
                  p.toOption.map(s => s.cluster.clusterMembers.size + s.cluster.hotSpares.size).getOrElse(0),
                  "hosts",
                  buildHostsSummaryTooltip(p.toOption),
                ),
              )
          else emptyNode,
        ),
      ),
      // Page-level "failed to refresh" banner — visible whenever any watched feed
      // is in FailedStale (a polling tick failed after data had already loaded).
      child.maybe <-- refreshFailedSignal.map { failed =>
        if (failed)
          Some(
            div(
              cls := "alert alert-warning mx-3 mt-3 mb-0",
              "Failed to refresh — showing last loaded data.",
            ),
          )
        else None
      },
      // Card grid
      div(
        cls := "container-fluid",
        // Row 1: Full-width system overview
        if (canSeeOverview)
          div(
            cls := "row px-3",
            div(
              cls := "col-12 mt-3",
              SystemOverviewCard(
                ingestsSignal,
                standingQueriesSignal,
                configSignal,
                metricsSignal,
                clusterStatusSignal = if (canSeeClusterHealth) clusterStatusSignal else None,
              ),
            ),
          )
        else emptyNode,
        // Row 2: Ingests + Standing Queries
        if (row2.nonEmpty) div(cls := "row px-3", row2) else emptyNode,
        // Row 3: Cluster Health (enterprise only) + Host Metrics
        if (row3.nonEmpty) div(cls := "row px-3", row3) else emptyNode,
        // Extra cards (e.g., License Usage for enterprise)
        allowedExtras.map { card =>
          div(cls := "row px-3", div(cls := "col-12 col-md-6 mt-3", card))
        },
      ),
    )
  }

  private def summaryBadge(count: Int, label: String, tooltipHtml: String): HtmlElement =
    span(
      cls := "d-inline-flex align-items-center me-3",
      styleAttr := "color: var(--thatdot-gradient-end); font-size: 1em; cursor: pointer;",
      LandingTooltip.attach[com.raquo.laminar.nodes.ReactiveHtmlElement.Base](tooltipHtml),
      span(
        cls := "badge",
        styleAttr := "background-color: rgba(255,255,255,0.2); margin-right: 0.4em;",
        count.toString,
      ),
      label,
    )

  private def buildIngestsSummaryTooltip(ingests: Seq[V2IngestInfo]): String = {
    if (ingests.isEmpty)
      return LandingTooltip.header("Ingests") +
      """<div style="color: #acacc9; font-size: 14px;">No ingests configured.</div>"""
    val running = ingests.count(_.status == "Running")
    val paused = ingests.count(i => i.status == "Paused" || i.status == "Restored")
    val failed = ingests.count(_.status == "Failed")
    val totalRate = ingests.map(_.stats.rates.oneMinute).sum
    LandingTooltip.header("Ingests") +
    LandingTooltip.kvTable(
      LandingTooltip.kvRow("Total", s"${ingests.size}") +
      LandingTooltip.kvRow("Total rate", LandingTooltip.formatRate(totalRate)) +
      LandingTooltip.kvRow("Running", s"$running", Some(LandingTooltip.statusColor("Running"))) +
      (if (paused > 0) LandingTooltip.kvRow("Paused/Restored", s"$paused", Some(LandingTooltip.statusColor("Paused")))
       else "") +
      (if (failed > 0) LandingTooltip.kvRow("Failed", s"$failed", Some(LandingTooltip.statusColor("Failed"))) else ""),
    )
  }

  private def buildQueriesSummaryTooltip(queries: Seq[V2StandingQueryInfo]): String = {
    if (queries.isEmpty)
      return LandingTooltip.header("Standing queries") +
      """<div style="color: #acacc9; font-size: 14px;">No standing queries configured.</div>"""
    val totalRate = queries.flatMap(_.stats.values).map(_.rates.oneMinute).sum
    val idle = queries.count(q => q.stats.isEmpty || q.stats.values.forall(_.rates.oneMinute <= 0))
    val active = queries.size - idle
    val totalOutputs = queries.map(_.outputs.size).sum
    LandingTooltip.header("Standing queries") +
    LandingTooltip.kvTable(
      LandingTooltip.kvRow("Total", s"${queries.size}") +
      LandingTooltip.kvRow("Active", s"$active", Some(LandingTooltip.statusColor("Running"))) +
      (if (idle > 0) LandingTooltip.kvRow("Idle", s"$idle") else "") +
      LandingTooltip.kvRow("Total match rate", LandingTooltip.formatRate(totalRate)) +
      LandingTooltip.kvRow("Total outputs", s"$totalOutputs"),
    )
  }

  private def buildHostsSummaryTooltip(statusOpt: Option[V2ServiceStatus]): String =
    statusOpt match {
      case None =>
        LandingTooltip.header("Cluster") +
          """<div style="color: #acacc9; font-size: 14px;">No cluster status available.</div>"""
      case Some(st) =>
        LandingTooltip.header("Cluster") +
          LandingTooltip.kvTable(
            LandingTooltip.kvRow(
              "Status",
              if (st.fullyUp) "Fully Up" else "Degraded",
              Some(if (st.fullyUp) LandingTooltip.statusColor("Running") else LandingTooltip.statusColor("Failed")),
            ) +
            LandingTooltip.kvRow("Members", s"${st.cluster.clusterMembers.size} / ${st.cluster.targetSize}") +
            LandingTooltip.kvRow("Hot spares", s"${st.cluster.hotSpares.size}"),
          )
    }
}
