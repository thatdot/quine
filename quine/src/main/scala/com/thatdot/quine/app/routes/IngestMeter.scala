package com.thatdot.quine.app.routes

import com.codahale.metrics.{Meter, Metered, Timer}

import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.graph.metrics.HostQuineMetrics

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

  /** Returns an ingest meter with meters retrieved or created based on the provided ingest name
    * @see com.codahale.metrics.MetricRegistry#meter
    */
  def ingestMeter(namespaceId: NamespaceId, name: String, metrics: HostQuineMetrics): IngestMeter =
    IngestMeter(
      name,
      namespaceId,
      metrics.metricRegistry.meter(metrics.metricName(namespaceId, List("ingest", name, "count"))),
      metrics.metricRegistry.meter(metrics.metricName(namespaceId, List("ingest", name, "bytes"))),
      metrics,
    )

  /** Removes any meters used in ingest meters for the provided ingest name
    * @see com.codahale.metrics.MetricRegistry#remove
    */
  def removeIngestMeter(namespaceId: NamespaceId, name: String, metrics: HostQuineMetrics): Boolean =
    metrics.metricRegistry.remove(metrics.metricName(namespaceId, List("ingest", name, "count"))) &&
    metrics.metricRegistry.remove(metrics.metricName(namespaceId, List("ingest", name, "bytes")))
}

final case class IngestMeter private[routes] (
  name: String,
  namespaceId: NamespaceId,
  countMeter: Meter, // mutable
  bytesMeter: Meter, // mutable
  private val metrics: HostQuineMetrics,
) extends IngestMetered {
  def mark(bytes: Int): Unit = {
    countMeter.mark()
    bytesMeter.mark(bytes.toLong)
  }
  override def counts: Metered = countMeter
  override def bytes: Metered = bytesMeter

  /** Returns a timer that can be used to track deserializations.
    * CAUTION this timer has different lifecycle behavior than the other metrics in this class.
    * See [[metrics.ingestDeserializationTimer]] for more information.
    * Note that not all ingest types use this timer.
    */
  def unmanagedDeserializationTimer: Timer =
    metrics.ingestDeserializationTimer(namespaceId, name)
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
