package com.thatdot.quine.graph

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder

import scala.collection.concurrent
import scala.jdk.CollectionConverters._

import com.codahale.metrics.{Counter, Gauge, Histogram, Meter, MetricFilter, MetricRegistry, Timer}

import com.thatdot.quine.util.SharedValve

// A MetricRegistry, wrapped with canonical accessors for common Quine metrics
final case class HostQuineMetrics(metricRegistry: MetricRegistry) {

  val nodePropertyCounter: BinaryHistogramCounter =
    BinaryHistogramCounter(metricRegistry, MetricRegistry.name("node", "property-counts"))
  val nodeEdgesCounter: BinaryHistogramCounter =
    BinaryHistogramCounter(metricRegistry, MetricRegistry.name("node", "edge-counts"))

  val persistorPersistEventTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("persistor", "persist-event"))
  val persistorPersistSnapshotTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("persistor", "persist-snapshot"))
  val persistorGetJournalTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("persistor", "get-journal"))
  val persistorGetLatestSnapshotTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("persistor", "get-latest-snapshot"))
  val persistorSetStandingQueryStateTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("persistor", "set-standing-query-state"))
  val persistorGetStandingQueryStatesTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("persistor", "get-standing-query-states"))

  def shardMessagesDeduplicatedCounter(shardName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("shard", shardName, "delivery-relay-deduplicated"))

  // Counters that track the sleep cycle (in aggregate) of nodes on the shard
  def shardNodesWokenUpCounter(shardName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("shard", shardName, "sleep-counters", "woken"))
  def shardNodesSleptSuccessCounter(shardName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("shard", shardName, "sleep-counters", "slept-success"))
  def shardNodesSleptFailureCounter(shardName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("shard", shardName, "sleep-counters", "slept-failure"))
  def shardNodesRemovedCounter(shardName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("shard", shardName, "sleep-counters", "removed"))

  // Counters that track occurences of supposedly unlikely (and generally bad) code paths
  def shardUnlikelyWakeupFailed(shardName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("shard", shardName, "unlikely", "wake-up-failed"))
  def shardUnlikelyIncompleteShdnCounter(shardName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("shard", shardName, "unlikely", "incomplete-shutdown"))
  def shardUnlikelyActorNameRsvdCounter(shardName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("shard", shardName, "unlikely", "actor-name-reserved"))
  def shardUnlikelyHardLimitReachedCounter(shardName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("shard", shardName, "unlikely", "hard-limit-reached"))
  def shardUnlikelyUnexpectedWakeUpErrCounter(shardName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("shard", shardName, "unlikely", "wake-up-error"))

  def removeShardMetrics(shardName: String): Unit =
    metricRegistry.removeMatching(MetricFilter.startsWith(MetricRegistry.name("shard", shardName)))

  /** Meter of results that were produced for a named standing query on this host */
  def standingQueryResultMeter(sqName: String): Meter =
    metricRegistry.meter(MetricRegistry.name("standing-queries", "results", sqName))

  /** Counter of results that were dropped for a named standing query on this host */
  def standingQueryDroppedCounter(sqName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("standing-queries", "dropped", sqName))

  /** Histogram of size (in bytes) of persisted standing query states */
  def standingQueryStateSize(sqId: StandingQueryId): Histogram =
    metricRegistry.histogram(MetricRegistry.name("standing-queries", "states", sqId.uuid.toString))

  private val standingQueryResultHashCodeRegistry: concurrent.Map[StandingQueryId, LongAdder] =
    new ConcurrentHashMap[StandingQueryId, LongAdder]().asScala

  def standingQueryResultHashCode(standingQueryId: StandingQueryId): LongAdder =
    standingQueryResultHashCodeRegistry.getOrElseUpdate(standingQueryId, new LongAdder)

  /** Histogram of size (in bytes) of persisted node snapshots */
  val snapshotSize: Histogram =
    metricRegistry.histogram(MetricRegistry.name("persistor", "snapshot-sizes"))

  /** Register a gauge tracking how many times a shared valve has been closed.
    *
    * @see [[SharedValve]] for details on this number
    * @param valve valve for which to create the gauge
    * @return registered gauge
    */
  def registerGaugeValve(valve: SharedValve): Gauge[Int] = {
    val gauge: Gauge[Int] = () => valve.getClosedCount
    metricRegistry.register(MetricRegistry.name("shared", "valve", valve.name), gauge)
  }
}
object HostQuineMetrics {

  val MetricsRegistryName = "quine-metrics"
}

/** Histogram where elements can be added or removed
  *
  * Hard-codes buckets for the following intervals:
  *
  *   - `[1, 8)`
  *   - `[8, 128)`
  *   - `[128, 2048)`
  *   - `[2048, 16384)`
  *   - `[16384, +Infinity)`
  */
class BinaryHistogramCounter(
  bucket1to8: Counter,
  bucket8to128: Counter,
  bucket128to2048: Counter,
  bucket2048to16384: Counter,
  bucket16384toInfinity: Counter
) {

  def increment(previousCount: Int): Unit =
    previousCount + 1 match {
      case 1 =>
        bucket1to8.inc()

      case 8 =>
        bucket1to8.dec()
        bucket8to128.inc()

      case 128 =>
        bucket8to128.dec()
        bucket128to2048.inc()

      case 2048 =>
        bucket128to2048.dec()
        bucket2048to16384.inc()

      case 16384 =>
        bucket2048to16384.dec()
        bucket16384toInfinity.inc()

      case _ => ()
    }

  def decrement(previousCount: Int): Unit =
    previousCount match {
      case 1 =>
        bucket1to8.dec()

      case 8 =>
        bucket1to8.inc()
        bucket8to128.dec()

      case 128 =>
        bucket8to128.inc()
        bucket128to2048.dec()

      case 2048 =>
        bucket128to2048.inc()
        bucket2048to16384.dec()

      case 16384 =>
        bucket2048to16384.inc()
        bucket16384toInfinity.dec()

      case _ => ()
    }
}

object BinaryHistogramCounter {

  def apply(
    registry: MetricRegistry,
    name: String
  ): BinaryHistogramCounter =
    new BinaryHistogramCounter(
      registry.counter(MetricRegistry.name(name, "1-7")),
      registry.counter(MetricRegistry.name(name, "8-127")),
      registry.counter(MetricRegistry.name(name, "128-2047")),
      registry.counter(MetricRegistry.name(name, "2048-16383")),
      registry.counter(MetricRegistry.name(name, "16384-infinity"))
    )
}
