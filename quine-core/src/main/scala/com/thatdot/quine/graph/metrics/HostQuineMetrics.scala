package com.thatdot.quine.graph.metrics

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder

import scala.collection.concurrent
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import com.codahale.metrics.{Counter, Histogram, Meter, MetricRegistry, NoopMetricRegistry, Timer}

import com.thatdot.quine.graph.metrics.implicits._
import com.thatdot.quine.graph.{NamespaceId, StandingQueryId, defaultNamespaceId}
import com.thatdot.quine.util.SharedValve

/** A MetricRegistry, wrapped with canonical accessors for common Quine metrics
  * @param enableDebugMetrics whether debugging-focused metrics should be included that have
  *                           a noticeable impact on runtime performance.
  * @param omitDefaultNamespace       Is this an enterprise instance? Used to determine naming conventions.
  * @param metricRegistry     the registry to wrap
  * @param hotNodes           settings for the hot-node gauges, see [[HotNodeSampler]]
  */
final case class HostQuineMetrics(
  enableDebugMetrics: Boolean,
  metricRegistry: MetricRegistry,
  omitDefaultNamespace: Boolean,
  hotNodes: HotNodeMetricsConfig = HotNodeMetricsConfig(),
) {
  import HostQuineMetrics._

  lazy val noOpRegistry: NoopMetricRegistry = new NoopMetricRegistry

  def metricName(namespaceId: NamespaceId, components: List[String]): String =
    if (omitDefaultNamespace && namespaceId == defaultNamespaceId)
      components.mkString(".")
    else
      (namespaceId.name :: components).mkString(".")

  /** Histogram tracking number of in-memory properties on nodes.
    */
  def nodePropertyCounter(namespaceId: NamespaceId): BinaryHistogramCounter =
    BinaryHistogramCounter(metricRegistry, metricName(namespaceId, List("node", "property-counts")))

  /** Histogram tracking number of in-memory edges on nodes. This tracks only in-memory edges, so supernodes past the
    * mitigation threshold (if enabled) will not be reflected.
    */
  def nodeEdgesCounter(namespaceId: NamespaceId): BinaryHistogramCounter =
    BinaryHistogramCounter(metricRegistry, metricName(namespaceId, List("node", "edge-counts")))

  /** Histogram tracking sizes of properties (in bytes) seen since startup. Unlike the node.property-counts
    * and node.edge-counts metrics, this metric does not attempt to track the current state of the system,
    * but rather aggregates statistics about the properties updates that have been seen, whether those properties
    * are currently in-memory or not.
    */
  def propertySizes(namespaceId: NamespaceId): Histogram =
    metricRegistry.histogram(metricName(namespaceId, List("node", "property-sizes")))

  /** Turning node state into snapshot bytes, and back again.
    *
    * Separate from the persist and fetch timers around them, which measure the store rather than
    * the codec. Serialization runs synchronously on the actor thread while the write that follows
    * it does not, so it is the part of snapshotting that competes with everything else the node
    * is doing, and the part a snapshot threshold actually avoids.
    */
  val snapshotSerializeTimer: Timer = metricRegistry.timer(MetricRegistry.name("persistor", "serialize-snapshot"))
  val snapshotDeserializeTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("persistor", "deserialize-snapshot"))

  val persistorPersistEventTimer: Timer = metricRegistry.timer(MetricRegistry.name("persistor", "persist-event"))
  val persistorPersistSnapshotTimer: Timer = metricRegistry.timer(MetricRegistry.name("persistor", "persist-snapshot"))
  val persistorGetJournalTimer: Timer = metricRegistry.timer(MetricRegistry.name("persistor", "get-journal"))
  val persistorGetLatestSnapshotTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("persistor", "get-latest-snapshot"))
  val persistorSetStandingQueryStateTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("persistor", "set-standing-query-state"))
  val persistorGetMultipleValuesStandingQueryStatesTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("persistor", "get-standing-query-states"))

  /** Metrics for a `history.*` procedure that answers by walking a node's journal itself.
    *
    * What such a call costs is governed by how much history it walks rather than by how long it
    * takes to answer, so the two counters are what make the cost visible: the ratio of journal
    * events read to rows reported is how selective the call's filters were, and a call that walks a
    * million events to report three rows looks the same as a cheap one in a timer alone.
    *
    * @param procedureName the procedure's Cypher name, such as `history.propertyChanges`
    */
  def journalWalkMetrics(procedureName: String): HostQuineMetrics.JournalWalkMetrics = {
    val name = historyMetricName(procedureName)
    HostQuineMetrics.JournalWalkMetrics(
      timer = metricRegistry.timer(MetricRegistry.name("history", name, "time")),
      journalEventsRead = metricRegistry.counter(MetricRegistry.name("history", name, "journal-events-read")),
      rowsReported = metricRegistry.counter(MetricRegistry.name("history", name, "rows-reported")),
    )
  }

  /** Metrics for a `history.*` procedure that answers by reading a node's state at a past moment.
    *
    * These rebuild a node through the ordinary wakeup path, which replays the journal below the
    * procedure rather than through a stream the procedure can observe. There is deliberately no
    * journal-events-read counter here: one would report zero for every call, which reads as "this
    * was free" rather than "this was not measured".
    *
    * @param procedureName the procedure's Cypher name, such as `history.nodeAt`
    */
  def stateReadMetrics(procedureName: String): HostQuineMetrics.StateReadMetrics = {
    val name = historyMetricName(procedureName)
    HostQuineMetrics.StateReadMetrics(
      timer = metricRegistry.timer(MetricRegistry.name("history", name, "time")),
      rowsReported = metricRegistry.counter(MetricRegistry.name("history", name, "rows-reported")),
    )
  }

  /** The metric-name component for a `history.*` procedure. The `history` prefix is supplied by the
    * registry, so it is stripped here rather than repeated as `history.history-...`.
    */
  private def historyMetricName(procedureName: String): String =
    procedureName.stripPrefix("history.").replace('.', '-')

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

  /** Every relay this shard deduplicated AGAINST: the denominator the deduplicated count has
    * never had. Without it "0 deduplicated" is unreadable: it means either nothing was ever
    * retransmitted, or every retransmission arrived after its entry had been evicted and was
    * re-executed as new. Those are opposite conclusions.
    */
  def shardMessagesRelayedCounter(shardName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("shard", shardName, "delivery-relay-received"))

  /** Attempts made by [[com.thatdot.quine.app.util.AtLeastOnceCypherQuery]] to run one query:
    * one per call plus one per retry. Its retry path logs only under `whenDebugEnabled`, so at
    * INFO a query that silently re-ran N times and re-applied its side effects N times is
    * indistinguishable from one that ran once. Compare this against the record count.
    */
  val cypherAtLeastOnceAttemptsCounter: Counter =
    metricRegistry.counter(MetricRegistry.name("cypher", "at-least-once-attempts"))

  /** Records the ingest data plane dispatched to a remote shard, and records a shard actually
    * executed on arrival. Two counters rather than one because they answer different questions:
    * if executed == dispatched but the graph shows a record applied twice, the duplication is
    * inside query execution; if executed > dispatched, it is in delivery; if dispatched exceeds
    * the source record count, the pump is dispatching twice.
    *
    * Dispatched increments on the pump's host and executed on the executing member's, so the
    * two compare only summed across the cluster, never per host. Executed normally runs a
    * little ahead of dispatched: a reopened channel re-sends its unacked tail and the member
    * re-executes it, so the excess is exactly the at-least-once redelivery volume.
    */
  val clusterIngestRecordsDispatchedCounter: Counter =
    metricRegistry.counter(MetricRegistry.name(ClusterIngestMetricComponent, "records-dispatched"))

  val clusterIngestRecordsExecutedCounter: Counter =
    metricRegistry.counter(MetricRegistry.name(ClusterIngestMetricComponent, "records-executed"))

  /** Entries pushed out of the dedup cache by newer ones. This is the failure mode made
    * visible: an evicted id can no longer be recognised, so its retransmission executes a
    * second time, silently. The cache is a fixed 10k entries, so the time it covers shrinks as
    * relay volume grows; eviction rate against the 2s retransmit interval is what says
    * whether the guarantee still holds.
    */
  def shardDedupEvictedCounter(shardName: String): Counter =
    metricRegistry.counter(MetricRegistry.name("shard", shardName, "delivery-relay-dedup-evicted"))

  // Meters that track relayAsk/relayTell messaging volume and latency
  val relayTellMetrics: RelayTellMetric =
    if (enableDebugMetrics) new DefaultRelayTellMetrics(metricRegistry) else NoOpMessageMetric
  val relayAskMetrics: RelayAskMetric =
    if (enableDebugMetrics) new DefaultRelayAskMetrics(metricRegistry) else NoOpMessageMetric

  // Metrics that track the sleep cycle (in aggregate) of nodes on the shard
  /** Counter of nodes that have been woken up on a shard, per-namespace */
  def shardNodesWokenUpCounter(namespaceId: NamespaceId, shardName: String): Counter =
    metricRegistry.counter(metricName(namespaceId, List("shard", shardName, "sleep-counters", "woken")))

  /** Counter of nodes that have been put to sleep on a shard, per-namespace */
  def shardNodesSleptSuccessCounter(namespaceId: NamespaceId, shardName: String): Counter =
    metricRegistry.counter(metricName(namespaceId, List("shard", shardName, "sleep-counters", "slept-success")))

  /** Counter of nodes that have failed to be put to sleep on a shard, per-namespace */
  def shardNodesSleptFailureCounter(namespaceId: NamespaceId, shardName: String): Counter =
    metricRegistry.counter(metricName(namespaceId, List("shard", shardName, "sleep-counters", "slept-failure")))

  /** Counter of nodes that have been removed from a shard (per-namespace) without a full sleep protocol */
  def shardNodesRemovedCounter(namespaceId: NamespaceId, shardName: String): Counter =
    metricRegistry.counter(metricName(namespaceId, List("shard", shardName, "sleep-counters", "removed")))

  /** Timer of how long it has taken to successfully sleep nodes on this shard, per-namespace */
  def shardNodesSleptTimer(namespace: NamespaceId, name: String): Timer =
    metricRegistry.timer(metricName(namespace, List("shard", name, "sleep-timers", "slept")))

  /** Timer of how long it has taken to successfully wake nodes on this shard, per-namespace */
  def shardNodesWokenTimer(namespace: NamespaceId, name: String): Timer =
    metricRegistry.timer(metricName(namespace, List("shard", name, "sleep-timers", "woken")))

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

  /** A timer tracking ingest query executions.
    * CAUTION: Unlike the other ingest-related metrics, this timer is not paused when the ingest is completed/failed.
    * This means its `Metered`-implementing metrics (pretty much anything with "rate" or "count" in the name) will
    * stale once ingest has stopped. Similarly, the metric will never be removed from the registry, so it may
    * accumulate stale data across namespace and other resets (IngestMeter is the exception to this, not the rule).
    * This metric also uses the naming scheme consistent with other metrics, NOT the naming scheme used for ingest
    * metrics.
    * @see [[com.thatdot.quine.app.routes.IngestMeter]]
    */
  def ingestQueryTimer(namespaceId: NamespaceId, ingestName: String): Timer =
    metricRegistry.timer(metricName(namespaceId, List(IngestMetricComponent, ingestName, "query")))

  /** A timer tracking ingest record deserialization time.
    * CAUTION: Unlike the other ingest-related metrics, this timer is not paused when the ingest is completed/failed.
    * This means its `Metered`-implementing metrics (pretty much anything with "rate" or "count" in the name) will
    * stale once ingest has stopped. Similarly, the metric will never be removed from the registry, so it may
    * accumulate stale data across namespace and other resets (IngestMeter is the exception to this, not the rule).
    * This metric also uses the naming scheme consistent with other metrics, NOT the naming scheme used for ingest
    * metrics.
    * @see [[com.thatdot.quine.app.routes.IngestMeter]]
    */
  def ingestDeserializationTimer(namespaceId: NamespaceId, ingestName: String): Timer =
    metricRegistry.timer(metricName(namespaceId, List(IngestMetricComponent, ingestName, "deserialization")))

  /** Meter of results that were produced for a named standing query on this host */
  def standingQueryResultMeter(namespaceId: NamespaceId, sqName: String): Meter =
    metricRegistry.meter {
      metricName(namespaceId, List(StandingQueryMetricComponent, "results", sqName))
    }

  /** Counter of results that were dropped for a named standing query on this host */
  def standingQueryDroppedCounter(namespaceId: NamespaceId, sqName: String): Counter =
    metricRegistry.counter {
      metricName(namespaceId, List(StandingQueryMetricComponent, "dropped", sqName))
    }

  /** Meter of cancellation (negative match) results enqueued for a named standing query on this host */
  def standingQueryCancellationMeter(namespaceId: NamespaceId, sqName: String): Meter =
    metricRegistry.meter {
      metricName(namespaceId, List(StandingQueryMetricComponent, "cancellations", sqName))
    }

  /** Meter of results consumed (leaving the queue) for a named standing query on this host.
    * This counts each result exactly once at the queue exit, before the BroadcastHub fans copies to outputs.
    */
  def standingQueryConsumptionMeter(namespaceId: NamespaceId, sqName: String): Meter =
    metricRegistry.meter {
      metricName(namespaceId, List(StandingQueryMetricComponent, "consumption", sqName))
    }

  /** Tracks how long SQ results spend in the result queue on this host before being accepted by each output
    * for processing. Due to the fan-out nature of the SQ results queue, a single publish to the results queue may
    * result in multiple measurements being counted against this timer (one for each sink on the SQ results hub, both
    * via declared outputs and via other sinks that are dynamically added like SSE and standing.wiretap).
    */
  def standingQueryResultQueueTimer(namespaceId: NamespaceId, name: String): Timer =
    metricRegistry.timer(metricName(namespaceId, List(StandingQueryMetricComponent, "queue-time", name)))

  /** Histogram of size (in bytes) of persisted standing query states */
  def standingQueryStateSize(namespaceId: NamespaceId, sqId: StandingQueryId): Histogram =
    metricRegistry.histogram(metricName(namespaceId, List(StandingQueryMetricComponent, "states", sqId.uuid.toString)))

  private val standingQueryResultHashCodeRegistry: concurrent.Map[StandingQueryId, LongAdder] =
    new ConcurrentHashMap[StandingQueryId, LongAdder]().asScala

  def standingQueryResultHashCode(standingQueryId: StandingQueryId): LongAdder =
    standingQueryResultHashCodeRegistry.getOrElseUpdate(standingQueryId, new LongAdder)

  /** Histogram of size (in bytes) of persisted node snapshots */
  val snapshotSize: Histogram =
    metricRegistry.histogram(MetricRegistry.name("persistor", "snapshot-sizes"))

  /** Snapshot bytes a waking node read back, or zero when it woke without one.
    *
    * The write-side counterpart above only sees nodes that wrote; this sees every wake, including
    * the ones a snapshot threshold turns into journal replay instead. Together with
    * `snapshot-economics.journal-events-replayed` it is what a wake cost to load.
    */
  val snapshotBytesRead: Histogram =
    metricRegistry.histogram(MetricRegistry.name("persistor", "snapshot-bytes-read"))

  /** Snapshots a waking node found stored under the other kind of history and moved to the key
    * this one writes. Climbs after the kind changes and settles once every node that had a
    * snapshot has woken since.
    */
  val snapshotsRekeyedOnWake: Counter =
    metricRegistry.counter(MetricRegistry.name("persistor", "snapshots-rekeyed-on-wake"))

  /** What each snapshot-on-sleep buys and what each wake pays.
    *
    * @see [[SnapshotEconomics]]. Gated on `enableDebugMetrics` because the wake-side hook forces
    * the restored journal to be counted.
    */
  val snapshotEconomics: SnapshotEconomics =
    new SnapshotEconomics(if (enableDebugMetrics) metricRegistry else noOpRegistry, enableDebugMetrics)

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

  /** What one `history.*` procedure records about a call.
    *
    * @param timer how long the call took, start to finish
    * @param journalEventsRead journal events walked, which is what the call actually costs
    * @param rowsReported rows the call produced, which is what the caller asked for
    */
  /** What every `history.*` procedure measures: how long a call took and how much it reported. */
  sealed trait HistoricalProcedureMetrics {
    def timer: Timer
    def rowsReported: Counter
  }

  /** A procedure that walks a journal itself, and so can also report how much of it it read. */
  final case class JournalWalkMetrics(
    timer: Timer,
    journalEventsRead: Counter,
    rowsReported: Counter,
  ) extends HistoricalProcedureMetrics

  /** A procedure that reads state at a past moment, whose journal cost is incurred below it. */
  final case class StateReadMetrics(
    timer: Timer,
    rowsReported: Counter,
  ) extends HistoricalProcedureMetrics

  val MetricsRegistryName = "quine-metrics"

  /** The two distributions that size `snapshotAfterEvents`. With journaling on, a snapshot only
    * bounds how much journal a wake replays, so a snapshot written after few events buys little.
    * Mass near zero in `events-since-snapshot` means the threshold is too low; a long tail in
    * `journal-events-replayed` means it is too high.
    */
  final class SnapshotEconomics(registry: MetricRegistry, val enabled: Boolean) {

    private def name(parts: String*): String =
      MetricRegistry.name("persistor", "snapshot-economics" +: parts: _*)

    /** Journal events each written snapshot was standing in for. */
    val eventsSinceSnapshot: Histogram = registry.histogram(name("events-since-snapshot"))

    /** Journal events replayed on wake. */
    val journalEventsReplayed: Histogram = registry.histogram(name("journal-events-replayed"))

    /** Record a snapshot written on node sleep.
      *
      * @param journaledEventsSinceSnapshot events journaled since this node's last snapshot; zero
      *                                     means the snapshot replaced nothing replayable at all
      */
    def recordSnapshotOnSleep(journaledEventsSinceSnapshot: Int): Unit =
      if (enabled) eventsSinceSnapshot.update(journaledEventsSinceSnapshot.toLong)

    /** Record the journal length replayed when a node woke. */
    def recordJournalReplayedOnWake(events: Int): Unit =
      if (enabled) journalEventsReplayed.update(events.toLong)
  }

  val IngestMetricComponent = "ingest"

  /** Metric component for cluster-wide dispatch/execution counters. Per-partition worker meters do
    * NOT live here -- they use [[IngestMetricComponent]], so `/metrics` redaction gates them on
    * `IngestRead` like any ingest. This component holds only the cluster-level counters, which the
    * same redaction gates on `IngestRead` too.
    */
  val ClusterIngestMetricComponent = "cluster-ingest"
  val StandingQueryMetricComponent = "standing-queries"

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

  sealed abstract class DefaultMessagingMetric(metricRegistry: MetricRegistry, val messageProtocol: String)
      extends MessagingMetric {
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

  val noOpTimer: Timer = new NoopMetricRegistry().timer("unused-timer-name")

  final object NoOpMessageMetric extends MessagingMetric with RelayAskMetric with RelayTellMetric {
    def markLocal(): Unit = ()

    def markRemote(): Unit = ()

    def markLocalFailure(): Unit = ()

    def markRemoteFailure(): Unit = ()

    def timeMessageSend[T](send: => Future[T]): Future[T] = send

    def timeMessageSend(): Timer.Context = noOpTimer.time()
  }
}
