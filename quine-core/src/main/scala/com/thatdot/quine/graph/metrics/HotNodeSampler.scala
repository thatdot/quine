package com.thatdot.quine.graph.metrics

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.control.NonFatal

import org.apache.pekko.actor.{ActorSystem, Cancellable}

import com.thatdot.common.logging.Log.{LogConfig, Safe, SafeLoggableInterpolator, StrictSafeLogging}
import com.thatdot.quine.graph.messaging.{NodeActorMailbox, SpaceTimeQuineId}
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.util.Log.implicits._

/** Settings for [[HotNodeSampler]]
  *
  * @param enabled        whether hot-node gauges are published at all
  * @param topN           how many nodes each ranking reports (the bound on cardinality: at most `3 * topN`
  *                       nodes, two gauges each)
  * @param minBacklog     a mailbox must have reached at least this many messages since the previous sample
  *                       to be ranked by backlog
  * @param minMessageRate a node must have received at least this many messages per second over the sample
  *                       window to be ranked by rate
  * @param minWaitSeconds a node's estimated wait (peak backlog divided by messages received per second over
  *                       the window, Little's law) must be at least this many seconds to be ranked by wait;
  *                       only nodes at or above `minBacklog` are considered, so a stray leftover message
  *                       does not read as a long wait
  * @param sampleInterval how often the rankings are recomputed
  */
final case class HotNodeMetricsConfig(
  enabled: Boolean = true,
  topN: Int = 10,
  minBacklog: Int = 10,
  minMessageRate: Double = 100.0,
  minWaitSeconds: Double = 1.0,
  sampleInterval: FiniteDuration = 10.seconds,
)

/** Publishes gauges naming the hottest node actors on this host, ranked three ways: by mailbox backlog
  * (the deepest queues), by message rate (the nodes taking the most traffic, whether or not they keep up),
  * and by estimated wait (the nodes falling furthest behind for their traffic).
  *
  * Every [[HotNodeMetricsConfig.sampleInterval]], the node mailboxes on this host are scanned once. Each
  * queue yields two numbers for the window since the previous sample: the deepest the mailbox got, and how
  * many messages arrived. Depth is a peak rather than the depth at sampling time because a hot node's queue
  * fills in a burst and drains within milliseconds, so an instantaneous reading is a coin toss while the
  * peak over the window identifies the node every time. Arrivals exclude
  * [[com.thatdot.quine.graph.behavior.StashedMessage]] re-deliveries, so a message that was paused once is
  * counted once.
  *
  * The [[HotNodeMetricsConfig.topN]] deepest queues at or above [[HotNodeMetricsConfig.minBacklog]], the
  * `topN` busiest at or above [[HotNodeMetricsConfig.minMessageRate]], and, among queues at or above
  * `minBacklog`, the `topN` with the longest estimated wait at or above
  * [[HotNodeMetricsConfig.minWaitSeconds]] are selected. Wait is Little's law,
  * peak backlog divided by arrival rate: the seconds a message spends in the mailbox. It is what separates a
  * node that is falling behind at a modest rate from a hub that is busy but keeping up, and it is why such a
  * node is published even when busier hubs fill the other two rankings. A node holding messages that received
  * none during the window is stalled; its rate is floored at one message per window, so its wait is its
  * backlog times the window and it ranks first. Every selected node, whichever ranking chose it, gets both
  * gauges:
  *
  *   - `<namespace>.node.mailbox-backlog.<id>`: peak mailbox depth over the window
  *   - `<namespace>.node.message-rate.<id>`: messages received per second over the window
  *
  * so a dashboard can show every ranking from the same two columns and derive the wait itself. Gauges of
  * nodes that are no longer selected are removed from the registry, so at most `3 * topN` nodes (two gauges
  * each) exist at any moment and metric reporters (JMX in particular) see the removal.
  *
  * The `<id>` component is the node's pretty ID as the ID provider renders it (what `strId(n)` returns in
  * Cypher), with every character outside `[A-Za-z0-9_-]` replaced by `_` so the name is safe for JMX
  * object names and reads the same through every reporter. Historical nodes carry an `-at-<millis>` suffix.
  *
  * The scan reads and resets each queue's two window counters; the message path pays one compare and two
  * counter increments per message.
  *
  * @param messageQueues every node mailbox on this host, keyed by node
  * @param metrics       the registry the gauges are published to, and the namespace naming rule
  * @param idProvider    renders node IDs; by-name because the graph's provider may not be built yet when the
  *                      sampler is constructed during graph initialization
  * @param config        bounds, thresholds, and cadence
  */
final class HotNodeSampler(
  messageQueues: ConcurrentHashMap[SpaceTimeQuineId, NodeActorMailbox.NodeMessageQueue],
  metrics: HostQuineMetrics,
  idProvider: => QuineIdProvider,
  config: HotNodeMetricsConfig,
)(implicit logConfig: LogConfig)
    extends StrictSafeLogging {
  import HotNodeSampler._

  /** The pair of gauges published for one node, holding the values of the latest window */
  final private class Published(val backlogName: String, val rateName: String) {
    @volatile var backlog: Long = 0L
    @volatile var rate: Double = 0.0
  }

  /** Nodes currently in the registry. Guarded by `this`. */
  private[this] val registered = mutable.Map.empty[SpaceTimeQuineId, Published]

  /** When the current window opened, so a sample can measure the real elapsed time rather than assume the
    * configured interval
    */
  @volatile private[this] var windowStartNanos: Long = System.nanoTime()

  /** Recompute the rankings over the window since the previous sample and reconcile the registry */
  def sample(): Unit = {
    val now = System.nanoTime()
    val windowSeconds = (now - windowStartNanos).toDouble / 1e9
    windowStartNanos = now
    sample(windowSeconds)
  }

  /** [[sample]] with the window length supplied, so a test can fix the rate denominator */
  private[metrics] def sample(windowSeconds: Double): Unit = if (config.enabled && config.topN > 0) {
    val selected = rankQueues(windowSeconds)

    synchronized {
      registered.keys.filterNot(selected.contains).toList.foreach { qid =>
        val published = registered(qid)
        metrics.metricRegistry.remove(published.backlogName)
        metrics.metricRegistry.remove(published.rateName)
        registered.remove(qid)
      }
      selected.foreach { case (qid, measurement) =>
        registered.get(qid) match {
          case Some(published) =>
            published.backlog = measurement.peak.toLong
            published.rate = measurement.rate
          case None => register(qid, measurement)
        }
      }
    }
  }

  private def register(qid: SpaceTimeQuineId, measurement: Measurement): Unit = {
    val published = new Published(metricName(qid, BacklogComponents), metricName(qid, RateComponents))
    published.backlog = measurement.peak.toLong
    published.rate = measurement.rate
    try {
      metrics.metricRegistry.registerGauge[Long](published.backlogName, () => published.backlog)
      metrics.metricRegistry.registerGauge[Double](published.rateName, () => published.rate)
      registered.put(qid, published)
      ()
    } catch {
      case _: IllegalArgumentException =>
        // Something else owns one of these names; leave both alone rather than fight over them.
        metrics.metricRegistry.remove(published.backlogName)
        logger.debug(safe"Skipping hot node gauges for ${Safe(published.backlogName)}: name already registered")
    }
  }

  /** Metric name under which one of `qid`'s gauges is published */
  private def metricName(qid: SpaceTimeQuineId, components: List[String]): String = {
    val pretty = idProvider.qidToPrettyString(qid.id)
    val withTime = qid.atTime.fold(pretty)(t => s"$pretty-at-${t.millis}")
    metrics.metricName(qid.namespace, components :+ sanitize(withTime))
  }

  /** The union of the `topN` deepest queues at or above `minBacklog`, the `topN` busiest at or above
    * `minMessageRate`, and the `topN` longest-waiting among queues at or above `minBacklog` whose wait is at
    * or above `minWaitSeconds`, with what was measured for each.
    *
    * Every queue's window counters are read and reset here, so the sampling interval is the window.
    */
  private def rankQueues(windowSeconds: Double): Map[SpaceTimeQuineId, Measurement] = {
    val seconds = Math.max(windowSeconds, MinimumWindowSeconds)
    val deepest = new Top(config.topN)(_.peak.toDouble)
    val busiest = new Top(config.topN)(_.rate)
    val slowest = new Top(config.topN)(_.waitSeconds)
    val entries = messageQueues.entrySet().iterator()
    while (entries.hasNext) {
      val entry = entries.next()
      val queue = entry.getValue
      val peak = queue.takePeakSize()
      val received = queue.takeReceived()
      lazy val measurement = Measurement(entry.getKey, peak, received.toDouble / seconds, windowSeconds = seconds)
      if (peak >= config.minBacklog) deepest.offer(measurement)
      if (measurement.rate >= config.minMessageRate) busiest.offer(measurement)
      if (peak >= config.minBacklog && measurement.waitSeconds >= config.minWaitSeconds) slowest.offer(measurement)
    }
    (deepest.result ++ busiest.result ++ slowest.result).map(m => m.qid -> m).toMap
  }

  /** Remove every gauge this sampler registered */
  def removeAll(): Unit = synchronized {
    registered.values.foreach { published =>
      metrics.metricRegistry.remove(published.backlogName)
      metrics.metricRegistry.remove(published.rateName)
    }
    registered.clear()
  }

  /** Begin sampling on the system scheduler. Returns an already-cancelled handle when disabled. */
  def start(system: ActorSystem): Cancellable =
    if (!config.enabled) Cancellable.alreadyCancelled
    else {
      windowStartNanos = System.nanoTime()
      system.scheduler.scheduleWithFixedDelay(config.sampleInterval, config.sampleInterval) { () =>
        // The scheduler stops repeating after an uncaught exception, so keep one bad sample from ending them all
        try sample()
        catch {
          case NonFatal(e) => logger.warn(log"Hot node sampling failed; will retry" withException e)
        }
      }(system.dispatcher)
    }
}

object HotNodeSampler {

  /** Name components between the namespace and the node ID, per gauge family */
  val BacklogComponents: List[String] = List("node", "mailbox-backlog")
  val RateComponents: List[String] = List("node", "message-rate")

  /** Guards the rate denominator against a zero-length window (two samples in the same instant) */
  private val MinimumWindowSeconds: Double = 1e-3

  /** What one scan measured for a node over the window */
  final private case class Measurement(qid: SpaceTimeQuineId, peak: Int, rate: Double, windowSeconds: Double) {

    /** Estimated seconds a message spends in the mailbox (Little's law: backlog over arrival rate).
      *
      * A window with no arrivals only says the rate is below one message per window, so that is the floor:
      * a node holding messages it received before the window is stalled, and its wait is its backlog times
      * the window rather than infinite.
      */
    def waitSeconds: Double =
      if (peak == 0) 0.0 else peak.toDouble / Math.max(rate, 1.0 / windowSeconds)
  }

  /** Replace every character that a metric name component must not contain */
  def sanitize(idComponent: String): String = idComponent.map { c =>
    if (c.isLetterOrDigit && c < 128 || c == '_' || c == '-') c else '_'
  }

  /** The `n` measurements with the highest `key`, kept as a bounded min-heap so memory stays O(n)
    * regardless of how many nodes are scanned. `result` is highest first.
    */
  final private class Top(n: Int)(key: Measurement => Double) {
    private val lowestFirst: Ordering[Measurement] = Ordering.by[Measurement, Double](key).reverse
    private val heap = mutable.PriorityQueue.empty[Measurement](lowestFirst)

    def offer(measurement: Measurement): Unit =
      if (heap.size < n) heap.enqueue(measurement)
      else if (key(measurement) > key(heap.head)) {
        heap.dequeue()
        heap.enqueue(measurement)
      }

    def result: List[Measurement] = {
      var highestFirst = List.empty[Measurement]
      while (heap.nonEmpty) highestFirst = heap.dequeue() :: highestFirst
      highestFirst
    }
  }
}
