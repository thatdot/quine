package com.thatdot.quine.app.routes

import java.util.concurrent.atomic.AtomicReference

import com.codahale.metrics.{Meter, Metered, Timer}

import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.graph.metrics.HostQuineMetrics.IngestMetricComponent
import com.thatdot.quine.graph.metrics.{HostQuineMetrics, IngestHostMetrics}

/** Like [[Metered]], but maintains multiple counters relevant to ingest
  */
sealed abstract class IngestMetered {
  def counts: Metered

  def bytes: Metered

  def getCount: Long = counts.getCount
}
object IngestMetered {

  /** Freeze a copy of the provided ingestMetered (ie, return a copy which will never change)
    * @param im the [[IngestMetered]] to freeze a copy of
    * @return the frozen copy
    */
  def freeze(im: IngestMetered): IngestMetered = new IngestMetered {
    override val counts: Metered = StoppedMeter.fromMeter(im.counts)
    override val bytes: Metered = StoppedMeter.fromMeter(im.bytes)
  }

  private def meterFor(
    component: String,
    namespaceId: NamespaceId,
    name: String,
    metrics: HostQuineMetrics,
  ): IngestMeter =
    IngestMeter(
      name,
      namespaceId,
      metrics.metricRegistry.meter(metrics.metricName(namespaceId, List(component, name, "count"))),
      metrics.metricRegistry.meter(metrics.metricName(namespaceId, List(component, name, "bytes"))),
      metrics,
      metricsAreRegistered = true,
    )

  /** Returns an ingest meter with meters retrieved or created based on the provided ingest name
    * @see com.codahale.metrics.MetricRegistry#meter
    */
  def ingestMeter(namespaceId: NamespaceId, name: String, metrics: HostQuineMetrics): IngestMeter =
    meterFor(IngestMetricComponent, namespaceId, name, metrics)

  /** Removes every metric registered for the provided ingest name
    * @see [[HostQuineMetrics.removeIngestMetrics]]
    */
  def removeIngestMeter(namespaceId: NamespaceId, name: String, metrics: HostQuineMetrics): Boolean =
    metrics.removeIngestMetrics(namespaceId, name)

  /** Returns an ingest meter whose metrics are private to the caller until it calls [[IngestMeter.register]].
    * For the window in which an ingest's configuration is still being validated: the meter can be threaded
    * through source construction and then abandoned, so a rejected configuration leaves nothing registered
    * to retire. A meter abandoned this way is ordinary garbage.
    */
  def unregisteredIngestMeter(namespaceId: NamespaceId, name: String, metrics: HostQuineMetrics): IngestMeter =
    IngestMeter(name, namespaceId, new Meter(), new Meter(), metrics, metricsAreRegistered = false)

  /** The meter for one cluster-ingest partition worker. Under [[IngestMetricComponent]], the same
    * as a member-local ingest, so `/metrics` redaction gates it on `IngestRead` like any other
    * ingest meter. It can share a name with a member-local ingest (`orders#2`), but that ingest
    * cannot be created while the worker runs -- the collision is refused at ingest creation
    * ([[com.thatdot.quine.app.QuineEnterpriseApp.addV2IngestStream]]), not kept apart by a separate
    * metric component.
    */
  def clusterPartitionMeter(namespaceId: NamespaceId, workerName: String, metrics: HostQuineMetrics): IngestMeter =
    meterFor(IngestMetricComponent, namespaceId, workerName, metrics)

  /** Removes every metric registered for the provided partition worker
    * @see [[clusterPartitionMeter]], [[HostQuineMetrics.removeIngestMetrics]]
    */
  def removeClusterPartitionMeter(namespaceId: NamespaceId, workerName: String, metrics: HostQuineMetrics): Boolean =
    metrics.removeIngestMetrics(namespaceId, workerName)
}

final case class IngestMeter private[routes] (
  name: String,
  namespaceId: NamespaceId,
  countMeter: Meter, // mutable
  bytesMeter: Meter, // mutable
  private val metrics: HostQuineMetrics,
  /** Whether these metrics are already in the registry ([[IngestMetered.ingestMeter]]) or are held
    * privately until [[register]] ([[IngestMetered.unregisteredIngestMeter]]).
    */
  private val metricsAreRegistered: Boolean,
) extends IngestMetered {

  /** The deserialization timer, once some source has asked for one. Held rather than re-resolved so that
    * [[register]] can publish the same instance the source is timing into.
    */
  private val deserializationTimer: AtomicReference[Timer] = new AtomicReference()

  def mark(bytes: Int): Unit = {
    countMeter.mark()
    bytesMeter.mark(bytes.toLong)
  }

  /** Record `n` bytes consumed from the input, independent of record framing. Use when the
    * site marking bytes doesn't naturally know record boundaries (e.g. a byte-source wireTap
    * upstream of a decoder that self-delimits).
    */
  def markBytes(n: Long): Unit = bytesMeter.mark(n)

  /** Record one ingested element, independent of byte accounting. Use when the site marking
    * records doesn't naturally know per-record byte size (e.g. a per-row wireTap downstream
    * of a decoder, where bytes were already counted upstream).
    */
  def markRecord(): Unit = countMeter.mark()

  override def counts: Metered = countMeter
  override def bytes: Metered = bytesMeter

  /** Returns a timer that can be used to track deserializations.
    * CAUTION this timer has different lifecycle behavior than the other metrics in this class.
    * See [[metrics.ingestDeserializationTimer]] for more information.
    * Note that not all ingest types use this timer, which is why asking for one is what brings it into
    * existence: an ingest that never times a deserialization registers no such metric at all.
    */
  def unmanagedDeserializationTimer: Timer =
    deserializationTimer.updateAndGet {
      case null if metricsAreRegistered => metrics.ingestDeserializationTimer(namespaceId, name)
      case null => new Timer()
      case existing => existing
    }

  /** Publish these metrics, making them visible to metrics reporting. Call once the ingest they belong to
    * is known to be starting; until then an [[IngestMetered.unregisteredIngestMeter]]'s metrics are private
    * to their creator, so abandoning it needs no cleanup. A no-op for a meter that was registered on
    * creation. The deserialization timer takes part only if some source asked for one.
    */
  def register(): Unit =
    if (!metricsAreRegistered) {
      metrics.registerIngestMetric(namespaceId, name, IngestHostMetrics.IngestCountComponent, countMeter)
      metrics.registerIngestMetric(namespaceId, name, IngestHostMetrics.IngestBytesComponent, bytesMeter)
      Option(deserializationTimer.get).foreach { timer =>
        metrics.registerIngestMetric(namespaceId, name, IngestHostMetrics.IngestDeserializationComponent, timer)
      }
    }
}

/** Meter that has been halted (so its rates/counts are no longer changing)
  *
  * This is handy for keeping track of rates of a stopped stream (completed or crashed), since we
  * don't want the rates to trend downwards after the stream has stopped.
  */
final case class StoppedMeter(
  getCount: Long,
  getFifteenMinuteRate: Double,
  getFiveMinuteRate: Double,
  getMeanRate: Double,
  getOneMinuteRate: Double,
) extends Metered
object StoppedMeter {
  def fromMeter(meter: Metered): Metered = StoppedMeter(
    meter.getCount,
    meter.getFifteenMinuteRate,
    meter.getFiveMinuteRate,
    meter.getMeanRate,
    meter.getOneMinuteRate,
  )
}
