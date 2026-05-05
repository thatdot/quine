package com.thatdot.quine.webapp.components.landing

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.{MetricsReport, ShardInMemoryLimit}
import com.thatdot.quine.webapp.components.landing.V2ApiTypes._
import com.thatdot.quine.webapp.util.Pot

/** Store for the landing page.
  *
  * Owns the Pot lifecycle state. Subscribes to the polling streams exposed by
  * [[LandingService]] and bridges each tick into a `Var[Pot[_]]`:
  *   - a successful tick replaces the Pot with `Ready(value)`
  *   - a failed tick surfaces as `Failed(msg)` on the initial fetch (no prior data),
  *     or as `FailedStale(prev, msg)` when prior data exists, so cards can keep
  *     showing the last known good value alongside a refresh-failure banner
  *
  * Stream subscriptions activate when [[subscriptions]] is mounted on an element.
  *
  * When `userPermissions` is `Some(_)`, fetches are skipped for data whose consuming
  * card the user cannot view. This avoids firing requests that would 403 and keeps
  * the browser console clean. `None` means no auth configured (OSS) — everything fetches.
  */
final class LandingStore(
  service: LandingService,
  fetchClusterStatus: Boolean = false,
  userPermissions: Option[Set[String]] = None,
) {

  type MetricsData = (MetricsReport, Map[Int, ShardInMemoryLimit])

  private val metricsVar: Var[Pot[MetricsData]] = Var(Pot.Empty)
  private val ingestsVar: Var[Pot[Seq[V2IngestInfo]]] = Var(Pot.Empty)
  private val standingQueriesVar: Var[Pot[Seq[V2StandingQueryInfo]]] = Var(Pot.Empty)
  private val clusterStatusVar: Var[Pot[V2ServiceStatus]] = Var(Pot.Empty)
  private val configVar: Var[Pot[V2QuineConfig]] = Var(Pot.Empty)

  val metricsSignal: Signal[Pot[MetricsData]] = metricsVar.signal
  val ingestsSignal: Signal[Pot[Seq[V2IngestInfo]]] = ingestsVar.signal
  val standingQueriesSignal: Signal[Pot[Seq[V2StandingQueryInfo]]] = standingQueriesVar.signal
  val clusterStatusSignal: Signal[Pot[V2ServiceStatus]] = clusterStatusVar.signal
  val configSignal: Signal[Pot[V2QuineConfig]] = configVar.signal

  private def allowed(needed: Set[String]): Boolean = LandingPage.hasPermissions(userPermissions, needed)

  // A datum is fetched only if at least one card that consumes it is viewable.
  // The overview persistor animation also uses the metrics feed for write rate/latency, so
  // allow metrics whenever that card can be seen and the user has ApplicationMetricsRead.
  private val fetchMetricsAllowed =
    allowed(HostMetricsCard.requiredPermissions) ||
    (allowed(SystemOverviewCard.requiredPermissions) && allowed(Set("ApplicationMetricsRead")))
  private val fetchIngestsAllowed =
    allowed(IngestsCard.requiredPermissions) || allowed(SystemOverviewCard.requiredPermissions)
  private val fetchStandingQueriesAllowed =
    allowed(StandingQueriesCard.requiredPermissions) || allowed(SystemOverviewCard.requiredPermissions)
  private val fetchConfigAllowed = allowed(SystemOverviewCard.requiredPermissions)
  private val fetchClusterStatusAllowed =
    fetchClusterStatus && allowed(ClusterHealthCard.requiredPermissions)

  /** Bind to a mounted element to activate stream subscriptions. On mount, Vars for
    * subscribed feeds are flipped to `Pending` so cards render a spinner until the
    * first tick lands.
    */
  val subscriptions: Modifier[HtmlElement] = {
    val binders = Seq.newBuilder[Modifier[HtmlElement]]
    if (fetchMetricsAllowed) binders ++= bind(service.metrics, metricsVar)
    if (fetchIngestsAllowed) binders ++= bind(service.ingests, ingestsVar)
    if (fetchStandingQueriesAllowed) binders ++= bind(service.standingQueries, standingQueriesVar)
    if (fetchClusterStatusAllowed) binders ++= bind(service.clusterStatus, clusterStatusVar)
    if (fetchConfigAllowed) binders ++= bind(service.config, configVar)
    binders += onMountCallback[HtmlElement] { _ =>
      if (fetchMetricsAllowed) metricsVar.set(Pot.Pending)
      if (fetchIngestsAllowed) ingestsVar.set(Pot.Pending)
      if (fetchStandingQueriesAllowed) standingQueriesVar.set(Pot.Pending)
      if (fetchClusterStatusAllowed) clusterStatusVar.set(Pot.Pending)
      if (fetchConfigAllowed) configVar.set(Pot.Pending)
    }
    binders.result()
  }

  /** Subscribe a feed to a Pot var.
    *
    *   - on a successful tick: set `Ready(value)`
    *   - on a failed tick: set `FailedStale(prev, msg)` if prior data exists (so cards
    *     keep showing it with a "Failed to refresh" banner), or `Failed(msg)` if there
    *     is no prior value
    */
  private def bind[A](feed: LandingService.Feed[A], v: Var[Pot[A]]): Seq[Modifier[HtmlElement]] = Seq(
    feed.values --> Observer[A](a => v.set(Pot.Ready(a))),
    feed.errors --> Observer[String] { msg =>
      v.update {
        case Pot.Ready(prev) => Pot.FailedStale(prev, msg)
        case Pot.FailedStale(prev, _) => Pot.FailedStale(prev, msg)
        case _ => Pot.Failed(msg)
      }
    },
  )
}
