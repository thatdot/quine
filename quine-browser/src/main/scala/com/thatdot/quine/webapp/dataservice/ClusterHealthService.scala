package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.Signal

import com.thatdot.quine.routes.{MetricsReport, ShardInMemoryLimit}
import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2ServiceStatus

/** Cluster-health capability: cluster status plus per-member telemetry (system metrics
  * and shard limits). Read-only — no commands.
  */
trait ClusterHealthService {

  /** Cluster status lifecycle; `FailedStale` keeps the last good status through transient
    * outages, and remains `Failed` where the status endpoint is unavailable (e.g. single-node OSS).
    */
  def clusterStatusSignal: Signal[Pot[V2ServiceStatus]]

  /** System metrics report (memory gauges, counters, timers).
    *
    * @param member cluster member position to read from ([[memberIndicesSignal]]);
    *               `None` reads the serving member. Implementations should share one
    *               underlying feed per member value, not one per call.
    */
  def metricsSignal(member: Option[Int]): Signal[Pot[MetricsReport]]

  /** Per-shard in-memory node limits, keyed by shard index.
    *
    * @param member cluster member position to read from; `None` reads the serving member.
    */
  def shardSizeLimitsSignal(member: Option[Int]): Signal[Pot[Map[Int, ShardInMemoryLimit]]]

  /** Health snapshot of one member (metrics zipped with shard limits), as consumed by the
    * landing page's host-metrics card (serving member) and the metrics dashboard (per member).
    *
    * @param member cluster member position to read from; `None` reads the serving member.
    *               Derived from the memoized per-member feeds, so repeated calls share polls.
    */
  final def hostMetricsSignal(member: Option[Int]): Signal[Pot[(MetricsReport, Map[Int, ShardInMemoryLimit])]] =
    metricsSignal(member).combineWith(shardSizeLimitsSignal(member)).map { case (m, s) => Pot.map2(m, s)((_, _)) }

  /** Sorted cluster member positions; empty when unclustered or status is unavailable. */
  lazy val memberIndicesSignal: Signal[Seq[Int]] =
    clusterStatusSignal.map(_.toOption.map(_.cluster.memberIndices).getOrElse(Seq.empty)).distinct
}
