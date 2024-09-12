package com.thatdot.quine.graph

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder

import scala.collection.concurrent
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import com.codahale.metrics.{Counter, Histogram, Meter, MetricRegistry, NoopMetricRegistry, Timer}

import com.thatdot.quine.graph.HostQuineMetrics.{
  DefaultRelayAskMetrics,
  DefaultRelayTellMetrics,
  NoOpMessageMetric,
  RelayAskMetric,
  RelayTellMetric,
}
import com.thatdot.quine.util.Log.{Safe, SafeLoggableInterpolator, StrictSafeLogging}
import com.thatdot.quine.util.SharedValve

/** A MetricRegistry, wrapped with canonical accessors for common Quine metrics
  * @param enableDebugMetrics whether debugging-focused metrics should be included that have
  *                           a noticeable impact on runtime performance.
  * @param isEnterprise       Is this an enterprise instance? Used to determine naming conventions.
  * @param metricRegistry     the registry to wrap
  */
final case class HostQuineMetrics(
  enableDebugMetrics: Boolean,
  metricRegistry: MetricRegistry,
  isEnterprise: Boolean = false,
) {
  lazy val noOpRegistry: NoopMetricRegistry = new NoopMetricRegistry

  // Elide the default namespace, and add the namespace as a prefix for all other namespaces (currently not used).
  private def standardName(namespaceId: NamespaceId, components: List[String]): String =
    namespaceId.fold(components)(ns => ns.name :: components).mkString(".")

  // Universally prefix namespaced metrics with the namespace name.
  private def enterpriseName(namespaceId: NamespaceId, components: List[String]): String =
    (namespaceToString(namespaceId) :: components).mkString(".")

  // TODO either remove this or use it universally. Customers using Grafana will need consideration.
  // Which convention should be used for metrics that are split by namespace?
  val metricName: (NamespaceId, List[String]) => String = if (isEnterprise) enterpriseName else standardName

  /** Histogram tracking number of in-memory properties on nodes.
    */
  def nodePropertyCounter(namespaceId: NamespaceId): BinaryHistogramCounter =
    BinaryHistogramCounter(metricRegistry, metricName(namespaceId, List("node", "property-counts")))

  /** Histogram tracking number of in-memory edges on nodes. This tracks only in-memory edges, so supernodes past the
    * mitigation threshold (if enabled) will not be reflected.
    */
  def nodeEdgesCounter(namespaceId: NamespaceId): BinaryHistogramCounter =
    BinaryHistogramCounter(metricRegistry, metricName(namespaceId, List("node", "edge-counts")))

  val persistorPersistEventTimer: Timer = metricRegistry.timer(MetricRegistry.name("persistor", "persist-event"))
  val persistorPersistSnapshotTimer: Timer = metricRegistry.timer(MetricRegistry.name("persistor", "persist-snapshot"))
  val persistorGetJournalTimer: Timer = metricRegistry.timer(MetricRegistry.name("persistor", "get-journal"))
  val persistorGetLatestSnapshotTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("persistor", "get-latest-snapshot"))
  val persistorSetStandingQueryStateTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("persistor", "set-standing-query-state"))
  val persistorGetMultipleValuesStandingQueryStatesTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("persistor", "get-standing-query-states"))

  /** @param context the context for which this timer is being used -- for
    *                example, "ingest-XYZ-deduplication" or "http-webpage-serve"
    */
  def cacheTimer(context: String): Timer =
    metricRegistry.timer(MetricRegistry.name("cache", context, "insert"))

  def shardNodeEvictionsMeter(namespaceId: NamespaceId, shardName: String): Meter =
    (if (enableDebugMetrics) metricRegistry else noOpRegistry).meter(
      metricName(namespaceId, List("shard", shardName, "nodes-evicted")),
    )

  def shardMessagesDeduplicatedCounter(shardName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("shard", shardName, "delivery-relay-deduplicated"))

  // Meters that track relayAsk/relayTell messaging volume and latency
  val relayTellMetrics: RelayTellMetric =
    if (enableDebugMetrics) new DefaultRelayTellMetrics(metricRegistry) else NoOpMessageMetric
  val relayAskMetrics: RelayAskMetric =
    if (enableDebugMetrics) new DefaultRelayAskMetrics(metricRegistry) else NoOpMessageMetric

  // Counters that track the sleep cycle (in aggregate) of nodes on the shard
  def shardNodesWokenUpCounter(namespaceId: NamespaceId, shardName: String): Counter =
    metricRegistry.counter(metricName(namespaceId, List("shard", shardName, "sleep-counters", "woken")))

  def shardNodesSleptSuccessCounter(namespaceId: NamespaceId, shardName: String): Counter =
    metricRegistry.counter(metricName(namespaceId, List("shard", shardName, "sleep-counters", "slept-success")))

  def shardNodesSleptFailureCounter(namespaceId: NamespaceId, shardName: String): Counter =
    metricRegistry.counter(metricName(namespaceId, List("shard", shardName, "sleep-counters", "slept-failure")))

  def shardNodesRemovedCounter(namespaceId: NamespaceId, shardName: String): Counter =
    metricRegistry.counter(metricName(namespaceId, List("shard", shardName, "sleep-counters", "removed")))

  // Counters that track occurrences of supposedly unlikely (and generally bad) code paths
  def shardUnlikelyWakeupFailed(namespaceId: NamespaceId, shardName: String): Counter =
    metricRegistry.counter(metricName(namespaceId, List("shard", shardName, "unlikely", "wake-up-failed")))

  def shardUnlikelyIncompleteShdnCounter(namespaceId: NamespaceId, shardName: String): Counter =
    metricRegistry.counter(metricName(namespaceId, List("shard", shardName, "unlikely", "incomplete-shutdown")))

  def shardUnlikelyActorNameRsvdCounter(namespaceId: NamespaceId, shardName: String): Counter =
    metricRegistry.counter(metricName(namespaceId, List("shard", shardName, "unlikely", "actor-name-reserved")))

  def shardUnlikelyHardLimitReachedCounter(namespaceId: NamespaceId, shardName: String): Counter =
    metricRegistry.counter(metricName(namespaceId, List("shard", shardName, "unlikely", "hard-limit-reached")))

  def shardUnlikelyUnexpectedWakeUpErrCounter(namespaceId: NamespaceId, shardName: String): Counter =
    metricRegistry.counter(metricName(namespaceId, List("shard", shardName, "unlikely", "wake-up-error")))

  /** Meter of results that were produced for a named standing query on this host */
  def standingQueryResultMeter(namespaceId: NamespaceId, sqName: String): Meter =
    metricRegistry.meter {
      metricName(namespaceId, List("standing-queries", "results", sqName))
    }

  /** Counter of results that were dropped for a named standing query on this host */
  def standingQueryDroppedCounter(namespaceId: NamespaceId, sqName: String): Counter =
    metricRegistry.counter {
      metricName(namespaceId, List("standing-queries", "dropped", sqName))
    }

  /** Histogram of size (in bytes) of persisted standing query states */
  def standingQueryStateSize(namespaceId: NamespaceId, sqId: StandingQueryId): Histogram =
    metricRegistry.histogram(metricName(namespaceId, List("standing-queries", "states", sqId.uuid.toString)))

  private val standingQueryResultHashCodeRegistry: concurrent.Map[StandingQueryId, LongAdder] =
    new ConcurrentHashMap[StandingQueryId, LongAdder]().asScala

  def standingQueryResultHashCode(standingQueryId: StandingQueryId): LongAdder =
    standingQueryResultHashCodeRegistry.getOrElseUpdate(standingQueryId, new LongAdder)

  /** Histogram of size (in bytes) of persisted node snapshots */
  val snapshotSize: Histogram =
    metricRegistry.histogram(MetricRegistry.name("persistor", "snapshot-sizes"))

  def registerGaugeDomainGraphNodeCount(size: () => Int): Unit = {
    metricRegistry.registerGauge(MetricRegistry.name("dgn-reg", "count"), () => size())
    ()
  }

  /** Register a gauge tracking how many times a shared valve has been closed.
    *
    * @see [[SharedValve]] for details on this number
    * @param valve valve for which to create the gauge
    * @return registered gauge
    */
  def registerGaugeValve(valve: SharedValve): Unit = {
    metricRegistry.registerGauge(MetricRegistry.name("shared", "valve", valve.name), () => valve.getClosedCount)
    ()
  }
}

object HostQuineMetrics {
  val MetricsRegistryName = "quine-metrics"

  sealed trait MessagingMetric {
    def markLocal(): Unit
    def markRemote(): Unit

    def markLocalFailure(): Unit
    def markRemoteFailure(): Unit

    def timeMessageSend[T](send: => Future[T]): Future[T]
    def timeMessageSend(): Timer.Context
  }
  sealed trait RelayAskMetric extends MessagingMetric
  sealed trait RelayTellMetric extends MessagingMetric

  sealed abstract class DefaultMessagingMetric(metricRegistry: MetricRegistry, val messageProtocol: String) {
    protected[this] val totalMeter: Meter =
      metricRegistry.meter(MetricRegistry.name("messaging", messageProtocol, "sent"))
    protected[this] val localMeter: Meter =
      metricRegistry.meter(MetricRegistry.name("messaging", messageProtocol, "sent", "local"))
    protected[this] val remoteMeter: Meter =
      metricRegistry.meter(MetricRegistry.name("messaging", messageProtocol, "sent", "remote"))
    // tracks time between initiating a message send and receiving an ack (or a result, if a result comes sooner)
    protected[this] val sendTimer: Timer =
      metricRegistry.timer(MetricRegistry.name("messaging", messageProtocol, "latency"))
    // tracks failed message sends (defined as in sendTimer)
    protected[this] val totalFailedSendMeter: Meter =
      metricRegistry.meter(MetricRegistry.name("messaging", messageProtocol, "failed"))
    protected[this] val localFailedSendMeter: Meter =
      metricRegistry.meter(MetricRegistry.name("messaging", messageProtocol, "failed", "local"))
    protected[this] val remoteFailedSendMeter: Meter =
      metricRegistry.meter(MetricRegistry.name("messaging", messageProtocol, "failed", "remote"))

    def markLocal(): Unit = {
      totalMeter.mark()
      localMeter.mark()
    }

    def markRemote(): Unit = {
      totalMeter.mark()
      remoteMeter.mark()
    }
    def markLocalFailure(): Unit = {
      totalFailedSendMeter.mark()
      localFailedSendMeter.mark()
    }
    def markRemoteFailure(): Unit = {
      totalFailedSendMeter.mark()
      remoteFailedSendMeter.mark()
    }

    def timeMessageSend[T](send: => Future[T]): Future[T] =
      sendTimer.time(send)

    def timeMessageSend(): Timer.Context = sendTimer.time()
  }
  final class DefaultRelayTellMetrics(metricRegistry: MetricRegistry)
      extends DefaultMessagingMetric(metricRegistry, "relayTell")
      with RelayTellMetric
  final class DefaultRelayAskMetrics(metricRegistry: MetricRegistry)
      extends DefaultMessagingMetric(metricRegistry, "relayAsk")
      with RelayAskMetric

  final object NoOpMessageMetric extends MessagingMetric with RelayAskMetric with RelayTellMetric {
    val noOpTimer: Timer = new com.codahale.metrics.NoopMetricRegistry().timer("unused-timer-name")
    def markLocal(): Unit = ()

    def markRemote(): Unit = ()

    def markLocalFailure(): Unit = ()

    def markRemoteFailure(): Unit = ()

    def timeMessageSend[T](send: => Future[T]): Future[T] = send

    def timeMessageSend(): Timer.Context = noOpTimer.time()
  }
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
  bucket16384toInfinity: Counter,
) extends StrictSafeLogging {

  /** Returns the counter that tracks how many instances of the provided `count` value exist.
    * Use only with great care -- for most of a resource's lifecycle, `increment` and `decrement` should be used.
    * Used to configure the histogram to track (or stop tracking) a value at a potentially-nonzero value,
    * for example, the number of properties on a node, which may be nonzero when the node is slept to persistence.
    */
  def bucketContaining(count: Int): Counter =
    if (count == 0) BinaryHistogramCounter.noopCounter
    else if (count < 0) {
      // This should never be hit, and indicates a bug.
      logger.info(
        safe"Negative count ${Safe(count.toString)} cannot be used with a binary histogram counter. Delegating to no-op counter instead.",
      )
      BinaryHistogramCounter.noopCounter
    } else if (count < 8) bucket1to8
    else if (count < 128) bucket8to128
    else if (count < 2048) bucket128to2048
    else if (count < 16384) bucket2048to16384
    else bucket16384toInfinity

  /** Adds a count to the appropriate bucket, managing transitions between buckets.
    * @param previousCount the _previous_ value of the count being incremented
    */
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

  /** Subtracts a count from the appropriate bucket, managing transitions between buckets.
    * @param previousCount the _previous_ value of the count being incremented
    */
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
  val noopCounter: Counter = new NoopMetricRegistry().counter("unused-counter-name")

  def apply(
    registry: MetricRegistry,
    name: String,
  ): BinaryHistogramCounter =
    new BinaryHistogramCounter(
      registry.counter(MetricRegistry.name(name, "1-7")),
      registry.counter(MetricRegistry.name(name, "8-127")),
      registry.counter(MetricRegistry.name(name, "128-2047")),
      registry.counter(MetricRegistry.name(name, "2048-16383")),
      registry.counter(MetricRegistry.name(name, "16384-infinity")),
    )
}
