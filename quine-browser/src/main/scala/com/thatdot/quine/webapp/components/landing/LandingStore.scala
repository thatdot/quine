package com.thatdot.quine.webapp.components.landing

import scala.concurrent.Future
import scala.util.{Failure, Success}

import com.raquo.laminar.api.L._
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes.{MetricsReport, ShardInMemoryLimit}
import com.thatdot.quine.webapp.components.landing.V2ApiTypes._
import com.thatdot.quine.webapp.util.Pot

/** Store for the landing page.
  *
  * Owns the command intake, Pot lifecycle state, and command processing.
  * Calls [[LandingService]] for async data fetching and bridges
  * the Future results back into the reactive state.
  *
  * Components push commands to [[input]].writer and read from the exposed signals.
  *
  * When `userPermissions` is `Some(_)`, fetches are skipped for data whose consuming card
  * the user cannot view. This avoids firing requests that would 403 and keeps the
  * browser console clean. `None` means no auth configured (OSS) — everything fetches.
  */
final class LandingStore(
  service: LandingService,
  fetchClusterStatus: Boolean = false,
  userPermissions: Option[Set[String]] = None,
) {

  type MetricsData = (MetricsReport, Map[Int, ShardInMemoryLimit])

  /** Command intake. Pass `input.writer` to components. */
  val input: EventBus[LandingCommand] = new EventBus

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

  /** Command processing — bind to a mounted element to activate.
    *
    * `Refresh` dispatches metrics, ingests, standing queries, and cluster status.
    * OSS deployments will get a failed `clusterStatus` Pot because `/api/v2/admin/status`
    * isn't exposed; the enterprise landing view is the one that actually renders it.
    */
  val commandHandler: Binder[HtmlElement] = input.events --> Observer[LandingCommand] {
    case LandingCommand.Refresh =>
      if (fetchMetricsAllowed) input.writer.onNext(LandingCommand.RefreshMetrics)
      if (fetchIngestsAllowed) input.writer.onNext(LandingCommand.RefreshIngests)
      if (fetchStandingQueriesAllowed) input.writer.onNext(LandingCommand.RefreshStandingQueries)
      if (fetchConfigAllowed) input.writer.onNext(LandingCommand.RefreshConfig)
      if (fetchClusterStatusAllowed) input.writer.onNext(LandingCommand.RefreshClusterStatus)

    case LandingCommand.RefreshMetrics =>
      if (fetchMetricsAllowed) refreshPot(metricsVar, service.fetchMetrics())

    case LandingCommand.RefreshIngests =>
      if (fetchIngestsAllowed) refreshPot(ingestsVar, service.fetchIngests())

    case LandingCommand.RefreshStandingQueries =>
      if (fetchStandingQueriesAllowed) refreshPot(standingQueriesVar, service.fetchStandingQueries())

    case LandingCommand.RefreshClusterStatus =>
      if (fetchClusterStatusAllowed) refreshPot(clusterStatusVar, service.fetchClusterStatus())

    case LandingCommand.RefreshConfig =>
      if (fetchConfigAllowed) refreshPot(configVar, service.fetchConfig())
  }

  private def refreshPot[A](v: Var[Pot[A]], fetch: => Future[A]): Unit = {
    val staleData = v.now().toOption
    v.set(staleData.fold[Pot[A]](Pot.Pending)(Pot.PendingStale(_)))
    fetch.onComplete {
      case Success(data) =>
        v.set(Pot.Ready(data))
      case Failure(err) =>
        val msg =
          if (err.getMessage != null && err.getMessage.nonEmpty) err.getMessage
          else "Fetch failed"
        v.set(staleData.fold[Pot[A]](Pot.Failed(msg))(Pot.FailedStale(_, msg)))
    }
  }
}
