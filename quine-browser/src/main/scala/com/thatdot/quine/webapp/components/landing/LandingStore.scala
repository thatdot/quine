package com.thatdot.quine.webapp.components.landing

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.{MetricsReport, ShardInMemoryLimit}
import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes._

/** Store for the landing page.
  *
  * Owns the Pot lifecycle state. Subscribes to the polling streams exposed by
  * [[LandingService]] and bridges each tick into a `Var[Pot[_]]`.
  *
  * Stream subscriptions activate when [[subscriptions]] is mounted on an element.
  */
final class LandingStore(
  service: LandingService,
  fetchClusterStatus: Boolean = false,
  userPermissions: Option[Set[String]] = None,
) {

  type MetricsData = (MetricsReport, Map[Int, ShardInMemoryLimit])

  private val metricsVar: Var[Pot[MetricsData]] = Var(Pot.Empty)
  private val clusterStatusVar: Var[Pot[V2ServiceStatus]] = Var(Pot.Empty)
  private val backpressureVar: Var[Pot[V2BackpressureSnapshot]] = Var(Pot.Empty)

  val metricsSignal: Signal[Pot[MetricsData]] = metricsVar.signal
  val clusterStatusSignal: Signal[Pot[V2ServiceStatus]] = clusterStatusVar.signal
  val backpressureSignal: Signal[Pot[V2BackpressureSnapshot]] = backpressureVar.signal

  private def allowed(needed: Set[String]): Boolean = LandingPage.hasPermissions(userPermissions, needed)

  private val fetchMetricsAllowed = allowed(HostMetricsCard.requiredPermissions)
  private val fetchClusterStatusAllowed =
    fetchClusterStatus && allowed(ClusterHealthCard.requiredPermissions)
  private val fetchBackpressureAllowed = allowed(Set("ApplicationMetricsRead"))

  /** Bind to a mounted element to activate stream subscriptions. */
  val subscriptions: Modifier[HtmlElement] = {
    val binders = Seq.newBuilder[Modifier[HtmlElement]]
    if (fetchMetricsAllowed) binders ++= bind(service.metrics, metricsVar)
    if (fetchClusterStatusAllowed) binders ++= bind(service.clusterStatus, clusterStatusVar)
    if (fetchBackpressureAllowed) binders ++= bind(service.backpressure, backpressureVar)
    binders += onMountCallback[HtmlElement] { _ =>
      if (fetchMetricsAllowed) metricsVar.set(Pot.Pending)
      if (fetchClusterStatusAllowed) clusterStatusVar.set(Pot.Pending)
      if (fetchBackpressureAllowed) backpressureVar.set(Pot.Pending)
    }
    binders.result()
  }

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
